"""
Bucket Browser: a local web GUI for object storage, built on cloud-files.

    python server.py                       # opens the browser, type a path there
    python server.py nokura://my_bucket/   # opens directly at that location

Supports every protocol cloud-files does (gs://, s3://, nokura://, matrix://,
file://, ...), using the same credentials in ~/.cloudvolume/secrets or
~/.cloudfiles/secrets. Only listens on 127.0.0.1.
"""
import argparse
import base64
import hashlib
import itertools
import json
import mimetypes
import multiprocessing
import os
import posixpath
import queue
import secrets
import threading
import time
import traceback
import uuid
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from contextlib import closing
from itertools import islice
from urllib.parse import parse_qs, quote, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
TRASH = ".Trash"
LIST_LIMIT = 20000   # max entries returned for one folder listing
MOVE_PROCESSES = min(6, os.cpu_count() or 1)  # worker processes for moves (--processes)
COPY_THREADS = 16    # concurrent server-side copies per worker (--threads)
MOVE_BATCH = 100     # objects handed to a worker at a time
DELETE_THREADS = 8   # parallel batch-delete requests
DELETE_BATCH = 1000  # keys per delete request (the S3 maximum)
LISTERS = 8          # folders listed in parallel while walking a tree
TOKEN = secrets.token_urlsafe(24)
FORMAT_PREFIXES = ("precomputed://", "graphene://", "boss://", "n5://", "zarr://", "zarr2://", "zarr3://")

mimetypes.add_type("image/webp", ".webp")


def _patch_monitor():
    """On Windows time.monotonic() ticks every ~15 ms, so a fast request can
    start and end on the same tick, and cloud-files' IntervalTree rejects the
    zero-length interval. Force every interval to be at least 1 us long."""
    try:
        from cloudfiles.monitoring import TransmissionMonitor
    except ImportError:
        return

    def end_io(self, flight_id, num_bytes):
        end_us = int(time.monotonic() * 1e6)
        with self._lock:
            start_us = int(self._in_flight.pop(flight_id) * 1e6)
            self._in_flight_bytes -= num_bytes
            self._intervaltree.addi(start_us, max(end_us, start_us + 1), [flight_id, num_bytes])
            self._total_bytes_landed += num_bytes

    if hasattr(TransmissionMonitor, "end_io"):
        TransmissionMonitor.end_io = end_io


_cloudfiles_ready = False


def cloudfiles_module():
    """Import cloud-files on first use. Windows starts each worker process by
    re-importing this file, and the S3 workers only need boto3, so keeping
    cloud-files (and its Google libraries) out of the module import lets them
    start quickly and use far less memory."""
    global _cloudfiles_ready
    import cloudfiles
    import cloudfiles.paths
    if not _cloudfiles_ready:
        _patch_monitor()
        _cloudfiles_ready = True
    return cloudfiles


def extract_path(path):
    return cloudfiles_module().paths.extract(path)


# ---------------------------------------------------------------- paths

def split_root(path):
    """'nokura://bucket/a/b' -> ('nokura://bucket/', 'a/b/').

    The root is the bucket (where .Trash lives). For file:// paths the
    given directory itself is treated as the root.
    """
    p = path.strip()
    for fmt in FORMAT_PREFIXES:
        if p.startswith(fmt):
            p = p[len(fmt):]
    if "://" not in p:
        p = "file://" + os.path.abspath(os.path.expanduser(p))
    e = extract_path(p)
    if e.protocol == "file":
        return "file://" + e.path.replace("\\", "/").rstrip("/") + "/", ""
    if not e.bucket:
        raise ValueError(f"No bucket in path: {path}")
    sub = (e.path or "").strip("/")
    body = p.rstrip("/")
    if sub and body.endswith(sub):
        body = body[: -len(sub)].rstrip("/")
    return body + "/", (sub + "/" if sub else "")


def check_rel(path):
    """Reject empty paths and '..' segments so nothing escapes the root."""
    if not path or path.startswith("/") and path.strip("/") == "":
        raise ValueError("Refusing to operate on an empty path")
    if ".." in path.split("/"):
        raise ValueError(f"Invalid path: {path}")
    return path


def parent_of(path):
    return path.rsplit("/", 1)[0] + "/" if "/" in path else ""


def basename_of(path):
    return path.rsplit("/", 1)[-1]


def in_trash(path):
    return path == TRASH or path.startswith(TRASH + "/")


def open_cf(root):
    return cloudfiles_module().CloudFiles(root, progress=False)


def local_base(root):
    return root[len("file://"):] if root.startswith("file://") else None


# ---------------------------------------------------------------- listing
# cloud-files' local backend is unreliable on Windows (backslash-only prefix
# matching, broken size=True), so file:// roots are listed with os directly.
# Everything else goes through cloud-files.

def iter_keys(cf, prefix):
    """Recursively yield object keys under prefix (relative to the root)."""
    base = local_base(cf.cloudpath)
    if base is None:
        yield from cf.list(prefix=prefix)
        return
    top = os.path.join(base, prefix)
    if not os.path.isdir(top):
        return
    for dirpath, _, filenames in os.walk(top):
        rel = os.path.relpath(dirpath, base).replace("\\", "/")
        rel = "" if rel == "." else rel + "/"
        for f in filenames:
            yield rel + f


def iter_flat(cf, prefix):
    """Yield (name, size) one level below prefix; folder names end with '/'."""
    base = local_base(cf.cloudpath)
    if base is None:
        for item in cf.list(prefix=prefix, flat=True, size=True):
            if isinstance(item, tuple):
                yield item[0], item[1]
            else:
                yield item, None
        return
    top = os.path.join(base, prefix)
    if not os.path.isdir(top):
        return
    with os.scandir(top) as it:
        for e in it:
            if e.is_dir():
                yield prefix + e.name + "/", None
            else:
                yield prefix + e.name, e.stat().st_size


def list_dir(root, prefix):
    cf = open_cf(root)
    dirs, files = {}, []
    truncated = False
    for n, (name, size) in enumerate(iter_flat(cf, prefix)):
        if n >= LIST_LIMIT:
            truncated = True
            break
        if name.startswith(prefix):
            name = name[len(prefix):]
        if name == "":
            continue  # the folder's own placeholder object
        if name.endswith("/"):
            dirs[name[:-1]] = True
        else:
            files.append({"name": name, "dir": False, "size": size})
    entries = [{"name": d, "dir": True, "size": None} for d in dirs] + files
    return {"root": root, "prefix": prefix, "entries": entries, "truncated": truncated}


def dir_exists(cf, path):
    base = local_base(cf.cloudpath)
    if base is not None:
        return os.path.isdir(os.path.join(base, path))
    return next(iter_flat(cf, path + "/"), None) is not None


def target_exists(cf, item, dest):
    if item["dir"]:
        return dir_exists(cf, dest)
    return bool(cf.exists(dest))


def name_taken(cf, path):
    return dir_exists(cf, path) or bool(cf.exists(path))


def make_marker(cf, dir_path):
    """Create an empty folder (dir_path has no trailing slash).

    Object storage can't hold an empty folder, so it is a zero-byte 'name/'
    marker object, as the AWS and Google consoles do it.
    """
    base = local_base(cf.cloudpath)
    if base is not None:
        os.makedirs(os.path.join(base, dir_path), exist_ok=True)
    else:
        cf.put(dir_path + "/", b"", content_type="application/x-directory")


def remove_empty_local_dirs(root, rel_dir):
    base = local_base(root)
    if base is None:
        return
    for dirpath, _, _ in os.walk(os.path.join(base, rel_dir), topdown=False):
        try:
            os.rmdir(dirpath)
        except OSError:
            pass


# ---------------------------------------------------------------- jobs
#
# Object storage has no rename: moving an object is always a server-side
# copy followed by a delete. To keep big folders fast, work is streamed:
# lister threads walk the folder tree and feed keys straight to a pool of
# copy/delete workers, so copying starts immediately rather than after the
# whole folder has been listed. Every job reports progress and can be
# cancelled at any point.

# Browsing comes first. While you open folders or view files, and for
# FOREGROUND_HOLD seconds after, background work yields: copies drop to
# BACKGROUND_COPIES per worker process, deletes to one request at a time, and
# listing pauses once enough work is queued. The storage server and this PC
# answer your clicks first, and full speed resumes by itself.

FOREGROUND_HOLD = 3.0
BACKGROUND_COPIES = 2
FOREGROUND_PATHS = {"/", "/index.html", "/api/list", "/api/file", "/api/text", "/api/resolve",
                    "/api/mkdir", "/api/write"}
_last_foreground = [0.0]
_limits = None           # shared int: copies allowed at once in each worker process
_governor_lock = threading.Lock()
_governor_running = False


def shared_limit():
    global _limits
    if _limits is None:
        _limits = multiprocessing.RawArray("i", [COPY_THREADS])
    return _limits


def foreground_active():
    return time.time() - _last_foreground[0] < FOREGROUND_HOLD


def note_foreground():
    """Called for every request that comes from you browsing."""
    global _governor_running
    _last_foreground[0] = time.time()
    shared_limit()[0] = BACKGROUND_COPIES
    with _governor_lock:
        if _governor_running:
            return
        _governor_running = True

    def restore():
        while True:
            time.sleep(0.25)
            shared_limit()[0] = BACKGROUND_COPIES if foreground_active() else COPY_THREADS

    threading.Thread(target=restore, daemon=True).start()


class Gate:
    """Caps how many operations run at once, following a limit that may
    change at any time (for copies it sits in memory shared with the worker
    processes)."""

    def __init__(self, limit):
        self.limit = limit
        self.active = 0
        self.cond = threading.Condition()

    def __enter__(self):
        with self.cond:
            while self.active >= max(1, self.limit()):
                self.cond.wait(0.05)
            self.active += 1

    def __exit__(self, *exc):
        with self.cond:
            self.active -= 1
            self.cond.notify_all()


_copy_gate = Gate(lambda: _limits[0] if _limits is not None else COPY_THREADS)
_delete_gate = Gate(lambda: 1 if foreground_active() else DELETE_THREADS)


def _lower_priority():
    """Worker processes run below normal priority so this PC stays responsive."""
    try:
        if os.name == "nt":
            import ctypes
            BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
            k32 = ctypes.windll.kernel32
            k32.SetPriorityClass(k32.GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS)
        else:
            os.nice(5)
    except Exception:
        pass


class Conflict(Exception):
    """Some files being moved have the same name as files at the destination."""

    def __init__(self, examples, count):
        super().__init__(f"{count:,} file(s) with the same name already exist at the destination")
        self.examples, self.count = examples, count


class Cancelled(Exception):
    pass


class Job:
    def __init__(self, op, label):
        self.id = uuid.uuid4().hex[:12]
        self.op = op
        self.label = label
        self.state = "running"   # running | done | error | conflict | cancelled
        self.phase = "Starting"
        self.message = ""
        self.cancel_note = ""
        self.found = 0           # objects discovered so far
        self.listing = True      # still discovering objects
        self.done = 0
        self.failed = 0
        self.skipped = 0         # same-name files left in place (merge with "skip")
        self.error = None
        self.conflicts = None
        self.nconflicts = 0
        self.cancel = threading.Event()
        self.started = time.time()
        self.work_started = None
        self._lock = threading.Lock()

    def add(self, done=0, found=0, failed=0, skipped=0):
        with self._lock:
            self.done += done
            self.found += found
            self.failed += failed
            self.skipped += skipped
            if self.work_started is None and (done or found):
                self.work_started = time.time()

    def check(self):
        if self.cancel.is_set():
            raise Cancelled()

    def to_dict(self):
        now = time.time()
        rate = eta = None
        if self.work_started and self.done:
            rate = self.done / max(now - self.work_started, 1e-3)
            if not self.listing:
                eta = max(0, self.found - self.done - self.failed - self.skipped) / rate
        running = self.state == "running"
        return {
            "id": self.id, "op": self.op, "label": self.label, "state": self.state,
            "phase": self.phase, "message": self.message, "found": self.found,
            "listing": self.listing, "done": self.done, "failed": self.failed,
            "skipped": self.skipped, "error": self.error, "conflicts": self.conflicts,
            "nconflicts": self.nconflicts, "elapsed": now - self.started, "rate": rate, "eta": eta,
            "cancelling": self.cancel.is_set() and running,
            "throttled": running and foreground_active(),
        }


JOBS = {}


def start_job(op, label, fn, *args):
    job = Job(op, label)
    JOBS[job.id] = job

    def run():
        try:
            fn(job, *args)
            job.state = "done"
        except Cancelled:
            job.state = "cancelled"
            job.message = f"Cancelled after {job.done:,} of {job.found:,} object(s). {job.cancel_note}".strip()
        except Conflict as c:
            job.state = "conflict"
            job.conflicts = c.examples
            job.nconflicts = c.count
            job.error = str(c)
        except Exception as ex:
            if not isinstance(ex, ValueError):     # ValueErrors are messages for the user
                traceback.print_exc()
            job.state = "error"
            job.error = f"{type(ex).__name__}: {ex}"
        job.listing = False

    threading.Thread(target=run, daemon=True).start()
    return job


def stream_walk(cf, job, prefix):
    """Yield ('key'|'marker', name) for everything under prefix ('a/b/').

    Several lister threads each take a folder, stream its listing page by
    page, and queue any subfolders they find for the other threads, so wide
    trees are listed in parallel and deep ones start producing keys at once.
    Use inside contextlib.closing() so the listers stop if the caller does.
    """
    base = local_base(cf.cloudpath)
    if base is not None:
        for dirpath, dirnames, filenames in os.walk(os.path.join(base, prefix)):
            job.check()
            rel = os.path.relpath(dirpath, base).replace("\\", "/") + "/"
            for f in filenames:
                yield "key", rel + f
            if not dirnames and not filenames:
                yield "marker", rel
        return

    out = queue.Queue(maxsize=20000)
    todo = queue.Queue()
    stop = threading.Event()
    lock = threading.Lock()
    pending = [0]
    seen = set()   # some backends repeat folder names in flat listings

    def halted():
        return stop.is_set() or job.cancel.is_set()

    def add_dir(p):
        with lock:
            if p in seen:
                return
            seen.add(p)
            pending[0] += 1
        todo.put(p)

    def emit(item):
        while not halted():
            if foreground_active() and out.qsize() >= 2000:
                time.sleep(0.1)      # you're browsing and there's work queued: stop listing
                continue
            try:
                out.put(item, timeout=0.2)
                return
            except queue.Full:
                pass

    def lister():
        while True:
            p = todo.get()
            if p is None:
                return
            try:
                if not halted():
                    for name, _ in iter_flat(cf, p):
                        if halted():
                            break
                        if name == p:
                            emit(("marker", name))
                        elif name.endswith("/"):
                            add_dir(name)
                        else:
                            emit(("key", name))
            except Exception as ex:
                emit(("error", ex))
            finally:
                with lock:
                    pending[0] -= 1
                    finished = pending[0] == 0
                if finished:
                    for _ in range(LISTERS):
                        todo.put(None)
                    emit(("end", None))

    add_dir(prefix)
    for _ in range(LISTERS):
        threading.Thread(target=lister, daemon=True).start()
    try:
        while True:
            job.check()
            try:
                kind, value = out.get(timeout=0.2)
            except queue.Empty:
                continue
            if kind == "end":
                return
            if kind == "error":
                raise value
            yield kind, value
    finally:
        stop.set()


def copy_key(cf, src, dest):
    """Server-side copy within the bucket (S3 CopyObject: the storage server
    copies the bytes itself; nothing is downloaded to this PC).

    cloud-files' copy_file() takes a second client from its connection pool
    for the destination and never returns it, so every copy built a new boto3
    client and opened a new TLS connection. That costs tens of ms of CPU here
    and a TLS handshake on the server per object. Use the pooled client instead.
    """
    with cf._get_connection() as conn:
        protocol = cf._path.protocol
        if protocol == "s3":
            import botocore.exceptions
            try:
                conn._conn.copy_object(
                    CopySource={"Bucket": cf._path.bucket, "Key": conn.get_path_to_file(src)},
                    Bucket=cf._path.bucket,
                    Key=conn.get_path_to_file(dest),
                    MetadataDirective="COPY",   # keep Content-Type / Content-Encoding
                    **(getattr(conn, "_additional_attrs", None) or {}),
                )
            except botocore.exceptions.ClientError as err:
                if err.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                    raise FileNotFoundError(src) from None
                raise
            return
        if protocol == "gs":
            from google.api_core import exceptions as gexc
            bucket = conn._bucket
            try:
                bucket.copy_blob(bucket.blob(conn.get_path_to_file(src)), bucket, conn.get_path_to_file(dest))
            except gexc.NotFound:
                raise FileNotFoundError(src) from None
            return
        found, _ = conn.copy_file(src, cf._path.bucket, posixpath.join(cf._path.path, dest))
    if not found:
        raise FileNotFoundError(src)


def _content_md5(request, **kwargs):
    """Dell ECS (nokura) rejects batch deletes that lack a Content-MD5 header,
    and boto3 >= 1.36 sends a CRC32 checksum instead. That is why cloud-files
    deletes nokura objects one request at a time. Swap the checksum for MD5,
    which every S3 implementation accepts."""
    body = request.body or b""
    if isinstance(body, str):
        body = body.encode("utf-8")
    for h in list(request.headers.keys()):
        if h.lower().startswith("x-amz-checksum-") or h.lower() == "x-amz-sdk-checksum-algorithm":
            del request.headers[h]
    request.headers["Content-MD5"] = base64.b64encode(hashlib.md5(body).digest()).decode()


def _s3_delete(client, bucket, keys, extra=None):
    """Batch-delete full keys, 1,000 per request. Returns 'key: reason'
    strings for keys the server refused to delete."""
    client.meta.events.register("before-sign.s3.DeleteObjects", _content_md5,
                                unique_id="bucket-browser-content-md5")
    failures = []
    for i in range(0, len(keys), DELETE_BATCH):
        resp = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in keys[i:i + DELETE_BATCH]], "Quiet": True},
            **(extra or {}),
        )
        failures += [f"{e.get('Key')}: {e.get('Code')} {e.get('Message', '')}".strip()
                     for e in resp.get("Errors", [])]
    return failures


def delete_keys(cf, keys):
    """Delete keys (relative to the root), 1,000 per request on S3. Returns
    'key: reason' strings for any keys the server refused to delete."""
    if not keys:
        return []
    with cf._get_connection() as conn:
        if cf._path.protocol != "s3":
            conn.delete_files(keys)
            return []
        return _s3_delete(conn._conn, cf._path.bucket, [conn.get_path_to_file(k) for k in keys],
                          getattr(conn, "_additional_attrs", None))


# Moves on real buckets run in worker processes. boto3 spends a few ms of CPU
# per request and Python threads share one core for that, so one process tops
# out around 300 requests/s however many threads it has; several processes
# scale close to linearly (measured on nokura). S3 workers use one plain boto3
# client each (thread-safe, with a pool of keep-alive connections) and never
# load cloud-files, so they start in a second or two.

_worker_cfs = {}
_s3_clients = {}
_s3_session = None
_s3_clients_lock = threading.Lock()
_cancel_flags = None     # one shared byte per job slot; 1 = skip remaining copies
_SKIPPED = object()


def _init_worker(flags, limits):
    global _cancel_flags, _limits
    _cancel_flags, _limits = flags, limits
    _lower_priority()
    _exit_with_parent()


def _exit_with_parent():
    """If the app is killed (not closed normally), Windows leaves its worker
    processes waiting for work forever. Watch the parent and exit with it."""
    import multiprocessing.connection
    parent = multiprocessing.parent_process()
    if parent is None:
        return

    def watch():
        try:
            multiprocessing.connection.wait([parent.sentinel])
        finally:
            os._exit(0)

    threading.Thread(target=watch, daemon=True).start()


def cancel_flags():
    global _cancel_flags
    if _cancel_flags is None:
        _cancel_flags = multiprocessing.RawArray("b", 1024)
    return _cancel_flags


def _worker_cf(root):
    cf = _worker_cfs.get(root)
    if cf is None:
        cf = _worker_cfs[root] = open_cf(root)
    return cf


def s3_conf(cf):
    """What a worker needs to reach this bucket with plain boto3, resolved the
    way cloud-files does it (secret files for aliases like nokura)."""
    from cloudfiles.secrets import aws_credentials
    creds = aws_credentials(cf._path.bucket, cf._path.alias or "s3") or {}
    return {
        "endpoint": cf._path.host, "bucket": cf._path.bucket, "prefix": cf._path.path or "",
        "key": creds.get("AWS_ACCESS_KEY_ID"), "secret": creds.get("AWS_SECRET_ACCESS_KEY"),
        "token": creds.get("AWS_SESSION_TOKEN") or None,
        "region": creds.get("AWS_DEFAULT_REGION") or "us-east-1",
    }


def _s3_client(conf, threads):
    global _s3_session
    key = (conf["endpoint"], conf["key"], conf["region"])
    with _s3_clients_lock:
        client = _s3_clients.get(key)
        if client is None:
            import boto3
            from botocore.config import Config
            if _s3_session is None:      # one session: its loaded service models are reused
                _s3_session = boto3.session.Session()
            client = _s3_session.client(
                "s3", endpoint_url=conf["endpoint"], region_name=conf["region"],
                aws_access_key_id=conf["key"], aws_secret_access_key=conf["secret"],
                aws_session_token=conf["token"],
                config=Config(max_pool_connections=max(10, threads + 4),
                              retries={"max_attempts": 6, "mode": "standard"}),
            )
            _s3_clients[key] = client
        return client


def _run_copies(pairs, threads, slot, copy):
    """copy(src, dest) for each pair, at most `threads` at once and within the
    shared limit. Copies not yet started when the job is cancelled are skipped.
    Returns (sources copied, error messages)."""
    flags = _cancel_flags

    def one(pair):
        if flags is not None and flags[slot]:
            return pair[0], _SKIPPED
        with _copy_gate:
            if flags is not None and flags[slot]:
                return pair[0], _SKIPPED
            try:
                copy(*pair)
                return pair[0], None
            except Exception as ex:
                return pair[0], f"{pair[0]}: {type(ex).__name__}: {ex}"

    ok, errors = [], []
    with ThreadPoolExecutor(max_workers=threads) as pool:
        for src, err in pool.map(one, pairs):
            if err is None:
                ok.append(src)
            elif err is not _SKIPPED:
                errors.append(err)
    return ok, errors


def _finish_batch(ok, errors, delete):
    """Delete the originals that were copied. Returns (moved, failed, messages)."""
    try:
        undeleted = delete(ok) if ok else []
    except Exception as ex:
        undeleted = [f"{k}: {ex}" for k in ok]
    moved = len(ok) - len(undeleted)
    failed = len(errors) + len(undeleted)
    errors += [f"copied, but the original could not be deleted: {m}" for m in undeleted]
    return moved, failed, errors[:5]


def _move_batch_s3(conf, pairs, threads, slot):
    """Move a batch on S3: CopyObject each (src, dest), then delete the
    sources whose copy succeeded in one request. Runs in a worker process."""
    import botocore.exceptions
    client = _s3_client(conf, threads)
    bucket, pre = conf["bucket"], conf["prefix"]
    full = (lambda k: posixpath.join(pre, k)) if pre else (lambda k: k)

    def copy(src, dest):
        try:
            client.copy_object(CopySource={"Bucket": bucket, "Key": full(src)}, Bucket=bucket,
                               Key=full(dest), MetadataDirective="COPY")
        except botocore.exceptions.ClientError as err:
            if err.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                raise FileNotFoundError(src) from None
            raise

    ok, errors = _run_copies(pairs, threads, slot, copy)
    return _finish_batch(ok, errors, lambda keys: _s3_delete(client, bucket, [full(k) for k in keys]))


def _move_batch(root, pairs, threads, slot):
    """The same for other backends (gs://, mem://), through cloud-files."""
    cf = _worker_cf(root)
    ok, errors = _run_copies(pairs, threads, slot, lambda src, dest: copy_key(cf, src, dest))
    return _finish_batch(ok, errors, lambda keys: delete_keys(cf, keys))


def batch_worker(cf):
    """(function, first argument) that moves one batch in this bucket."""
    if cf._path.protocol == "s3":
        return _move_batch_s3, s3_conf(cf)
    return _move_batch, cf.cloudpath


def _warm(target, threads):
    """Load libraries and create the client in a worker process ahead of time."""
    if isinstance(target, dict):
        _s3_client(target, threads)
    else:
        with _worker_cf(target)._get_connection():
            pass


_pool = None
_pool_lock = threading.Lock()
_job_slots = itertools.count()


def uses_processes(root):
    return MOVE_PROCESSES > 1 and (root.startswith("s3://") or root.startswith("gs://")
                                   or extract_path(root).protocol in ("s3", "gs"))


def process_pool():
    """Returns (pool, freshly_started)."""
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ProcessPoolExecutor(max_workers=MOVE_PROCESSES, initializer=_init_worker,
                                        initargs=(cancel_flags(), shared_limit()))
            return _pool, True
        return _pool, False


_warmed = {}   # root -> worker processes asked to start for it


def warm_workers(root, n=None):
    """Start worker processes ahead of time; each takes a few seconds on
    Windows. A couple start when you open a bucket, so a paste begins moving
    at once; the rest start when you cut something (or when a move needs them)."""
    n = MOVE_PROCESSES if n is None else min(n, MOVE_PROCESSES)
    if not uses_processes(root):
        return
    with _pool_lock:
        have = _warmed.get(root, 0)
        if have >= n:
            return
        _warmed[root] = n
    pool, _ = process_pool()
    _, target = batch_worker(open_cf(root))
    for _ in range(n - have):
        pool.submit(_warm, target, COPY_THREADS)


def warm_soon(root, n):
    if _warmed.get(root, 0) < min(n, MOVE_PROCESSES):
        threading.Thread(target=warm_workers, args=(root, n), daemon=True).start()


def move_executor(cf):
    """Process pool for real buckets; threads for mem:// (which lives in
    this process) or when --processes 1. Returns (executor, owned, fresh)."""
    if not uses_processes(cf.cloudpath):
        return ThreadPoolExecutor(max_workers=4), True, False
    pool, fresh = process_pool()
    return pool, False, fresh


def _reset_pool():
    global _pool
    with _pool_lock:
        if _pool is not None:
            _pool.shutdown(wait=False, cancel_futures=True)
        _pool = None
        _warmed.clear()


class Workers:
    """A thread pool with a bounded backlog. Listing may run ahead of the
    workers by up to `backlog` tasks, which lets the total (and so the ETA)
    become known early without holding millions of keys in memory."""

    def __init__(self, n, backlog):
        self.pool = ThreadPoolExecutor(max_workers=n)
        self.slots = threading.BoundedSemaphore(backlog)

    def submit(self, job, fn, *args):
        while not self.slots.acquire(timeout=0.2):
            job.check()
        fut = self.pool.submit(fn, *args)
        fut.add_done_callback(lambda _: self.slots.release())

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.pool.shutdown(wait=True)


def remote_move(job, cf, plan):
    """plan: [(src, dest, is_dir, skip)]. Objects are listed here and handed
    to workers in batches; each worker copies its batch server-side, then
    deletes the originals whose copy succeeded. `skip` is None or a set of
    paths (relative to the folder) that exist at the destination and must be
    left alone."""
    executor, owned, fresh = move_executor(cf)
    fn, target = batch_worker(cf)
    backlog = 2 * (4 if owned else MOVE_PROCESSES)   # batches in flight
    flags = cancel_flags()
    slot = next(_job_slots) % len(flags)
    flags[slot] = 0
    inflight, markers, messages = {}, [], []
    batch = []

    def sync_cancel():
        if job.cancel.is_set():
            flags[slot] = 1          # workers skip copies they haven't started

    def collect(fut):
        n = inflight.pop(fut)
        try:
            moved, failed, msgs = fut.result()
        except Exception as ex:
            if isinstance(ex, BrokenProcessPool):
                _reset_pool()
            moved, failed, msgs = 0, n, [f"worker failed: {type(ex).__name__}: {ex}"]
        job.phase = "Moving"
        job.add(done=moved, failed=failed)
        messages.extend(msgs)

    def drain(limit):
        while len(inflight) > limit:
            done, _ = wait(list(inflight), timeout=0.2, return_when=FIRST_COMPLETED)
            for f in done:
                collect(f)
            sync_cancel()

    def submit():
        nonlocal batch
        while len(inflight) >= backlog:
            drain(backlog - 1)
            job.check()
        inflight[executor.submit(fn, target, batch, COPY_THREADS, slot)] = len(batch)
        batch = []

    def add(src, dest):
        job.add(found=1)
        batch.append((src, dest))
        if len(batch) >= MOVE_BATCH:
            submit()

    job.phase = "Starting worker processes" if fresh else "Moving"
    job.cancel_note = "Objects not yet moved are still in the original location."
    try:
        for src, dest, is_dir, skip in plan:
            if not is_dir:
                add(src, dest)
                continue
            with closing(stream_walk(cf, job, src + "/")) as items:
                for kind, name in items:
                    rel = name[len(src) + 1:]
                    if kind == "marker":
                        markers.append((name, (dest + "/" + rel).rstrip("/")))
                    elif skip is not None and rel in skip:
                        job.add(found=1, skipped=1)
                    else:
                        add(name, dest + "/" + rel)
        job.check()
        if batch:
            submit()
        job.listing = False
    finally:
        # Wait for batches already handed out. Each object ends up either
        # fully moved or untouched at the source (after a cancel, workers
        # skip the copies they haven't started).
        sync_cancel()
        drain(0)
        if owned:
            executor.shutdown(wait=True)

    job.check()           # cancelled after everything was queued
    for old, new in markers:
        make_marker(cf, new)
    if markers and not job.failed:
        delete_keys(cf, [old for old, _ in markers])
    if job.failed:
        raise RuntimeError(f"{job.failed:,} object(s) could not be moved. Anything that failed to copy "
                           f"is still at the source. First error: {messages[0] if messages else 'unknown'}")


def _retry(fn, *args):
    """Windows briefly denies renames while antivirus or the search indexer
    has a just-written file open; retry for a couple of seconds."""
    for attempt in range(12):
        try:
            return fn(*args)
        except PermissionError:
            if os.name != "nt" or attempt == 11:
                raise
            time.sleep(0.05 * (attempt + 1))


def local_move(job, root, plan):
    """file:// moves are real renames: instant, even for whole folders."""
    base = local_base(root)
    job.phase = "Moving"
    job.cancel_note = "Items not yet moved are still in the original location."
    for src, dest, is_dir, skip in plan:
        job.check()
        s, d = os.path.join(base, src), os.path.join(base, dest)
        os.makedirs(os.path.dirname(d), exist_ok=True)
        if not os.path.exists(d):
            job.add(found=1)
            _retry(os.rename, s, d)
            job.add(done=1)
            continue
        if not is_dir:            # a same-name file the user chose to replace
            job.add(found=1)
            _retry(os.replace, s, d)
            job.add(done=1)
            continue
        for dirpath, _, filenames in os.walk(s):   # merge into the existing folder
            rel_dir = os.path.relpath(dirpath, s).replace("\\", "/")
            target_dir = os.path.join(d, rel_dir)
            os.makedirs(target_dir, exist_ok=True)
            for f in filenames:
                job.check()
                rel = f if rel_dir == "." else f"{rel_dir}/{f}"
                if skip is not None and rel in skip:
                    job.add(found=1, skipped=1)
                    continue
                job.add(found=1)
                _retry(os.replace, os.path.join(dirpath, f), os.path.join(target_dir, f))
                job.add(done=1)
        remove_empty_local_dirs(root, src)
    job.listing = False


def existing_files(cf, job, folder):
    """Set of file paths (relative to folder) already in a destination folder."""
    found = set()
    with closing(stream_walk(cf, job, folder + "/")) as items:
        for kind, name in items:
            if kind == "key":
                found.add(name[len(folder) + 1:])
                if len(found) % 5000 == 0:
                    job.phase = f"Checking for files with the same name ({len(found):,} checked)"
    return found


def relocate(job, root, mapping, on_collision="ask"):
    """Move items. mapping: [(item, new_path)] with paths relative to root.

    Moving onto an existing folder merges the two: files with different names
    on either side are always kept. Files with the same name follow
    on_collision: 'ask' stops with a Conflict listing them (only if there are
    any), 'replace' overwrites them, 'skip' leaves them in the original folder.
    """
    cf = open_cf(root)
    base = local_base(root)
    job.phase = "Checking destination"
    plan, clashes, nclash = [], [], 0
    for item, dest in mapping:
        job.check()
        src = check_rel(item["path"])
        check_rel(dest)
        if src == dest:
            continue
        if item["dir"] and (dest + "/").startswith(src + "/"):
            raise ValueError(f"Cannot move folder '{src}' into itself")
        skip = None
        if not item["dir"]:
            if on_collision != "replace":
                exists = (os.path.isfile(os.path.join(base, dest)) if base is not None
                          else bool(cf.exists(dest)))
                if exists and on_collision == "skip":
                    job.add(found=1, skipped=1)
                    continue
                if exists:
                    nclash += 1
                    clashes.append(dest)
        elif on_collision != "replace" and dir_exists(cf, dest):
            job.phase = "Checking for files with the same name"
            existing = existing_files(cf, job, dest)
            if existing and on_collision == "skip":
                skip = existing
            elif existing:
                with closing(stream_walk(cf, job, src + "/")) as items:
                    for kind, name in items:
                        rel = name[len(src) + 1:]
                        if kind == "key" and rel in existing:
                            nclash += 1
                            if len(clashes) < 5:
                                clashes.append(f"{dest}/{rel}")
        plan.append((src, dest, item["dir"], skip))
    if nclash:
        raise Conflict(clashes[:5], nclash)
    if base is not None:
        local_move(job, root, plan)
    else:
        remote_move(job, cf, plan)
    job.message = f"Moved {job.done:,} object(s)"
    if job.skipped:
        job.message += (f"; skipped {job.skipped:,} with the same name as files already there "
                        f"(they're still in the original folder)")


def op_move(job, root, items, dest_prefix, on_collision):
    if dest_prefix:
        check_rel(dest_prefix.rstrip("/"))
    mapping = [(it, dest_prefix + basename_of(it["path"])) for it in items]
    relocate(job, root, mapping, on_collision)


def op_rename(job, root, item, new_name):
    check_name(new_name)
    dest = parent_of(item["path"]) + new_name
    if dest != item["path"] and name_taken(open_cf(root), dest):
        raise ValueError(f"'{new_name}' already exists here. To merge folders, cut and paste instead.")
    relocate(job, root, [(item, dest)], "replace")


def op_trash(job, root, items):
    cf = open_cf(root)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    mapping = []
    for it in items:
        check_rel(it["path"])
        if in_trash(it["path"]):
            raise ValueError("Already in .Trash; use Ctrl+Delete to delete permanently")
        dest = f"{TRASH}/{it['path']}"
        if target_exists(cf, it, dest):
            stem, ext = (dest, "") if it["dir"] else os.path.splitext(dest)
            dest = f"{stem}~{stamp}{ext}"
        mapping.append((it, dest))
    relocate(job, root, mapping, "replace")


def op_restore(job, root, items, on_collision):
    mapping = []
    for it in items:
        if not it["path"].startswith(TRASH + "/"):
            raise ValueError(f"Not in .Trash: {it['path']}")
        mapping.append((it, it["path"][len(TRASH) + 1:]))
    relocate(job, root, mapping, on_collision)


def op_delete(job, root, items):
    cf = open_cf(root)
    base = local_base(root)
    job.phase = "Deleting"
    job.cancel_note = "Objects not yet deleted were left untouched."
    for it in items:
        check_rel(it["path"])

    if base is not None:
        for it in items:
            p = os.path.join(base, it["path"])
            if not it["dir"]:
                job.check()
                job.add(found=1)
                os.remove(p)
                job.add(done=1)
                continue
            for dirpath, _, filenames in os.walk(p):
                for f in filenames:
                    job.check()
                    job.add(found=1)
                    os.remove(os.path.join(dirpath, f))
                    job.add(done=1)
            remove_empty_local_dirs(root, it["path"])
        job.listing = False
        job.message = f"Deleted {job.done:,} object(s)"
        return

    failures = []

    def delete_batch(keys):
        with _delete_gate:          # one request at a time while you browse
            if job.cancel.is_set():
                return
            try:
                refused = delete_keys(cf, keys)
            except Exception as ex:
                failures.append(f"{type(ex).__name__}: {ex}")
                job.add(failed=len(keys))
                return
        failures.extend(refused[:5])
        job.add(done=len(keys) - len(refused), failed=len(refused))

    with Workers(DELETE_THREADS, backlog=50) as workers:
        batch = []
        for it in items:
            if not it["dir"]:
                batch.append(it["path"])
                job.add(found=1)
            else:
                with closing(stream_walk(cf, job, it["path"] + "/")) as entries:
                    for _, name in entries:      # keys and folder markers alike
                        batch.append(name)
                        job.add(found=1)
                        if len(batch) >= DELETE_BATCH:
                            workers.submit(job, delete_batch, batch)
                            batch = []
        if batch:
            workers.submit(job, delete_batch, batch)
        job.listing = False
    job.check()
    if failures:
        raise RuntimeError(f"{job.failed:,} object(s) could not be deleted. First error: {failures[0]}")
    job.message = f"Deleted {job.done:,} object(s)"


# ---------------------------------------------------------------- create / edit

TEXT_LIMIT = 5 * 1024 * 1024   # largest file the text editor will open


def check_name(name):
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        raise ValueError(f"Invalid name: {name!r}")
    return name


def create_folder(root, prefix, name):
    cf = open_cf(root)
    path = check_rel(prefix + check_name(name))
    if name_taken(cf, path):
        raise ValueError(f"'{name}' already exists here")
    make_marker(cf, path)
    return {"path": path}


def read_text(root, path):
    check_rel(path)
    cf = open_cf(root)
    size = cf.size(path)
    if size is None:
        raise ValueError(f"Not found: {path}")
    if size > TEXT_LIMIT:
        raise ValueError(f"File is {size / 2**20:.1f} MB; the editor opens files up to {TEXT_LIMIT // 2**20} MB")
    data = cf.get(path) or b""
    if b"\0" in data[:8192]:
        raise ValueError("This looks like a binary file, not text")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("File is not UTF-8 text")
    return {"content": text.replace("\r\n", "\n"), "crlf": "\r\n" in text, "size": len(data)}


def write_text(root, path, content, crlf=False, create=False):
    check_rel(path)
    cf = open_cf(root)
    if create:
        check_name(basename_of(path))
        if name_taken(cf, path):
            raise ValueError(f"'{basename_of(path)}' already exists here")
    if crlf:
        content = content.replace("\r\n", "\n").replace("\n", "\r\n")
    data = content.encode("utf-8")
    ctype = mimetypes.guess_type(path)[0] or "text/plain"
    if ctype.startswith("text/"):
        ctype += "; charset=utf-8"
    cf.put(path, data, content_type=ctype)
    return {"path": path, "size": len(data)}


# ---------------------------------------------------------------- HTTP

class Handler(BaseHTTPRequestHandler):
    server_version = "BucketBrowser/1.0"

    def log_message(self, fmt, *args):
        pass

    # DNS-rebinding guard: only answer requests addressed to localhost.
    def host_ok(self):
        host = (self.headers.get("Host") or "").rsplit(":", 1)[0]
        return host in ("127.0.0.1", "localhost")

    def send_json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if not self.host_ok():
            return self.send_error(403)
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path in FOREGROUND_PATHS:
            note_foreground()        # you're browsing: background work yields
        try:
            if url.path in ("/", "/index.html"):
                return self.serve_index()
            if url.path == "/api/file":
                if q.get("t") != TOKEN:
                    return self.send_error(403)
                return self.serve_file(q["root"], q["path"], q.get("download") == "1")
            if self.headers.get("X-Token") != TOKEN:
                return self.send_error(403)
            if url.path == "/api/resolve":
                root, prefix = split_root(q["path"])
                return self.send_json({"root": root, "prefix": prefix})
            if url.path == "/api/text":
                return self.send_json(read_text(q["root"], q["path"]))
            if url.path == "/api/list":
                result = list_dir(q["root"], q.get("prefix", ""))
                warm_soon(q["root"], 2)      # so a later paste starts moving at once
                return self.send_json(result)
            if url.path == "/api/jobs":
                return self.send_json({"jobs": [j.to_dict() for j in JOBS.values() if j.state == "running"]})
            if url.path == "/api/job":
                job = JOBS.get(q.get("id"))
                return self.send_json(job.to_dict() if job else {"error": "unknown job"},
                                      200 if job else 404)
            self.send_error(404)
        except Exception as ex:
            traceback.print_exc()
            self.send_json({"error": f"{type(ex).__name__}: {ex}"}, 500)

    def do_POST(self):
        if not self.host_ok() or self.headers.get("X-Token") != TOKEN:
            return self.send_error(403)
        try:
            n = int(self.headers.get("Content-Length") or 0)
            req = json.loads(self.rfile.read(n) or b"{}")
            if self.path in FOREGROUND_PATHS:
                note_foreground()
            if self.path == "/api/warm":
                threading.Thread(target=warm_workers, args=(req["root"],), daemon=True).start()
                return self.send_json({"ok": True})
            if self.path == "/api/cancel":
                job = JOBS.get(req.get("id"))
                if job:
                    job.cancel.set()
                return self.send_json({"ok": bool(job)})
            if self.path == "/api/mkdir":
                return self.send_json(create_folder(req["root"], req.get("prefix", ""), req["name"]))
            if self.path == "/api/write":
                return self.send_json(write_text(req["root"], req["path"], req.get("content", ""),
                                                 bool(req.get("crlf")), bool(req.get("create"))))
            op, root = req.get("op"), req.get("root")
            items = req.get("items", [])
            on_collision = req.get("on_collision", "ask")
            if on_collision not in ("ask", "replace", "skip"):
                return self.send_json({"error": f"bad on_collision: {on_collision}"}, 400)
            label = req.get("label", op)
            if op == "trash":
                job = start_job(op, label, op_trash, root, items)
            elif op == "delete":
                job = start_job(op, label, op_delete, root, items)
            elif op == "move":
                job = start_job(op, label, op_move, root, items, req["dest"], on_collision)
            elif op == "rename":
                job = start_job(op, label, op_rename, root, items[0], req["name"])
            elif op == "restore":
                job = start_job(op, label, op_restore, root, items, on_collision)
            else:
                return self.send_json({"error": f"unknown op {op}"}, 400)
            self.send_json({"job": job.id})
        except Exception as ex:
            traceback.print_exc()
            self.send_json({"error": f"{type(ex).__name__}: {ex}"}, 500)

    def serve_index(self):
        with open(os.path.join(HERE, "index.html"), "rb") as f:
            body = f.read().replace(b"__TOKEN__", TOKEN.encode())
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def serve_file(self, root, path, download):
        check_rel(path)
        data = open_cf(root).get(path)
        if data is None:
            return self.send_json({"error": "not found"}, 404)
        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "private, no-cache")
        if download:
            self.send_header("Content-Disposition",
                             f"attachment; filename*=UTF-8''{quote(basename_of(path))}")
        self.end_headers()
        self.wfile.write(data)


def main():
    global COPY_THREADS, MOVE_PROCESSES
    ap = argparse.ArgumentParser(description="Local web GUI for cloud-files buckets")
    ap.add_argument("path", nargs="?", help="bucket path to open, e.g. nokura://my_bucket/")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--processes", type=int, default=MOVE_PROCESSES,
                    help=f"worker processes for moves on real buckets (default {MOVE_PROCESSES})")
    ap.add_argument("--threads", type=int, default=COPY_THREADS,
                    help=f"concurrent server-side copies per worker process (default {COPY_THREADS})")
    args = ap.parse_args()
    COPY_THREADS = max(1, args.threads)
    MOVE_PROCESSES = max(1, args.processes)

    server = None
    for port in range(args.port, args.port + 20):
        try:
            server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
            break
        except OSError:
            continue
    if server is None:
        raise SystemExit("No free port found")

    url = f"http://127.0.0.1:{server.server_port}/"
    if args.path:
        url += "#path=" + quote(args.path, safe="")
    print(f"Bucket Browser running at {url}\nPress Ctrl+C to stop.")
    if not args.no_browser:
        threading.Timer(0.5, webbrowser.open, (url,)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
