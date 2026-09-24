"""
Bucket Browser: a local web GUI for object storage, built on cloud-files.

    python server.py                       # opens the browser, type a path there
    python server.py nokura://my_bucket/   # opens directly at that location

Supports every protocol cloud-files does (gs://, s3://, nokura://, matrix://,
file://, ...), using the same credentials in ~/.cloudvolume/secrets or
~/.cloudfiles/secrets. Only listens on 127.0.0.1.
"""
import argparse
import json
import mimetypes
import os
import secrets
import threading
import time
import traceback
import uuid
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from urllib.parse import parse_qs, quote, urlparse

from cloudfiles import CloudFiles
from cloudfiles import paths as cfpaths

HERE = os.path.dirname(os.path.abspath(__file__))
TRASH = ".Trash"
LIST_LIMIT = 20000   # max entries returned for one folder listing
CHUNK = 100          # objects per move/delete batch (for progress reporting)
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


_patch_monitor()


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
    e = cfpaths.extract(p)
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
    return CloudFiles(root, progress=False)


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


def walk(cf, prefix):
    """Everything under prefix ('a/b/'), as (object_keys, folder_markers).

    Folder markers are the zero-byte 'name/' objects that make empty folders
    show up on object storage (for file:// they are empty directories).
    cloud-files' recursive listing skips them, so remote folders are walked
    one level at a time, with the folders of each level listed in parallel.
    """
    base = local_base(cf.cloudpath)
    keys, markers = [], []
    if base is not None:
        for dirpath, dirnames, filenames in os.walk(os.path.join(base, prefix)):
            rel = os.path.relpath(dirpath, base).replace("\\", "/") + "/"
            keys += [rel + f for f in filenames]
            if not dirnames and not filenames:
                markers.append(rel)
        return keys, markers

    keys, markers = {}, {}   # dicts as ordered sets: some backends repeat entries
    level = [prefix]
    with ThreadPoolExecutor(max_workers=16) as pool:
        while level:
            nxt = {}
            for p, entries in zip(level, pool.map(lambda p: list(iter_flat(cf, p)), level)):
                for name, _ in entries:
                    if name == p:
                        markers[name] = True
                    elif name.endswith("/"):
                        nxt[name] = True
                    else:
                        keys[name] = True
            level = list(nxt)
    return list(keys), list(markers)


def expand(cf, item):
    """(object_keys, folder_markers) belonging to an item {'path', 'dir'}."""
    if item["dir"]:
        return walk(cf, item["path"] + "/")
    return [item["path"]], []


def dir_exists(cf, path):
    base = local_base(cf.cloudpath)
    if base is not None:
        return os.path.isdir(os.path.join(base, path))
    return next(iter_flat(cf, path + "/"), None) is not None


def dir_sample(cf, path, n=5):
    return list(islice(iter_keys(cf, path + "/"), n)) or [path + "/"]


def target_exists(cf, item, dest):
    if item["dir"]:
        return dir_exists(cf, dest)
    return bool(cf.exists(dest))


def name_taken(cf, path):
    return dir_exists(cf, path) or bool(cf.exists(path))


def make_marker(cf, dir_path):
    """Create an empty folder (dir_path has no trailing slash)."""
    base = local_base(cf.cloudpath)
    if base is not None:
        os.makedirs(os.path.join(base, dir_path), exist_ok=True)
    else:
        cf.put(dir_path + "/", b"", content_type="application/x-directory")


# ---------------------------------------------------------------- jobs

class Conflict(Exception):
    def __init__(self, examples, count):
        super().__init__(f"{count} object(s) already exist at the destination")
        self.examples, self.count = examples, count


class Job:
    def __init__(self, op):
        self.id = uuid.uuid4().hex[:12]
        self.op = op
        self.state = "running"   # running | done | error | conflict
        self.message = "Starting..."
        self.done = 0
        self.total = None
        self.error = None
        self.conflicts = None
        self.nconflicts = 0

    def to_dict(self):
        return {k: getattr(self, k) for k in
                ("id", "op", "state", "message", "done", "total", "error", "conflicts", "nconflicts")}


JOBS = {}


def start_job(op, fn, *args):
    job = Job(op)
    JOBS[job.id] = job

    def run():
        try:
            fn(job, *args)
            job.state = "done"
        except Conflict as c:
            job.state = "conflict"
            job.conflicts = c.examples
            job.nconflicts = c.count
            job.error = str(c)
        except Exception as ex:
            traceback.print_exc()
            job.state = "error"
            job.error = f"{type(ex).__name__}: {ex}"

    threading.Thread(target=run, daemon=True).start()
    return job


def remove_empty_local_dirs(root, rel_dir):
    base = local_base(root)
    if base is None:
        return
    top = os.path.join(base, rel_dir)
    for dirpath, _, _ in os.walk(top, topdown=False):
        try:
            os.rmdir(dirpath)
        except OSError:
            pass


def relocate(job, root, mapping, overwrite):
    """Move items. mapping: [(item, new_path)] with paths relative to root."""
    cf = open_cf(root)
    pairs, old_markers, new_markers, moved_dirs = [], [], [], []
    job.message = "Checking..."
    for item, dest in mapping:
        src = check_rel(item["path"])
        check_rel(dest)
        if src == dest:
            continue
        if item["dir"] and (dest + "/").startswith(src + "/"):
            raise ValueError(f"Cannot move folder '{src}' into itself")
        if not overwrite and target_exists(cf, item, dest):
            raise Conflict(dir_sample(cf, dest) if item["dir"] else [dest], 1)
        keys, markers = expand(cf, item)
        if item["dir"]:
            pairs += [(k, dest + k[len(src):]) for k in keys]
            old_markers += markers
            new_markers += [(dest + m[len(src):]).rstrip("/") for m in markers]
            moved_dirs.append(src)
        else:
            pairs.append((src, dest))

    job.total = len(pairs) + len(new_markers)
    job.message = "Moving"
    for i in range(0, len(pairs), CHUNK):
        chunk = pairs[i:i + CHUNK]
        cf.moves(cf, chunk, block_size=CHUNK)
        job.done += len(chunk)

    for m in new_markers:
        make_marker(cf, m)
        job.done += 1
    if local_base(root) is None:
        if old_markers:
            cf.delete(old_markers)
    else:
        for src in moved_dirs:
            remove_empty_local_dirs(root, src)
    job.message = f"Moved {job.total} object(s)"


def op_move(job, root, items, dest_prefix, overwrite):
    if dest_prefix:
        check_rel(dest_prefix.rstrip("/"))
    mapping = [(it, dest_prefix + basename_of(it["path"])) for it in items]
    relocate(job, root, mapping, overwrite)


def op_rename(job, root, item, new_name, overwrite):
    if not new_name or "/" in new_name or new_name in (".", ".."):
        raise ValueError(f"Invalid name: {new_name!r}")
    relocate(job, root, [(item, parent_of(item["path"]) + new_name)], overwrite)


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
    relocate(job, root, mapping, overwrite=True)


def op_restore(job, root, items, overwrite):
    mapping = []
    for it in items:
        if not it["path"].startswith(TRASH + "/"):
            raise ValueError(f"Not in .Trash: {it['path']}")
        mapping.append((it, it["path"][len(TRASH) + 1:]))
    relocate(job, root, mapping, overwrite)


def op_delete(job, root, items):
    cf = open_cf(root)
    job.message = "Listing..."
    keys = []
    for it in items:
        check_rel(it["path"])
        k, markers = expand(cf, it)
        keys += k
        if local_base(root) is None:
            keys += markers
    job.total = len(keys)
    job.message = "Deleting"
    for i in range(0, len(keys), CHUNK):
        chunk = keys[i:i + CHUNK]
        cf.delete(chunk)
        job.done += len(chunk)
    for it in items:
        if it["dir"]:
            remove_empty_local_dirs(root, it["path"])
    job.message = f"Deleted {len(keys)} object(s)"


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
                return self.send_json(list_dir(q["root"], q.get("prefix", "")))
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
            if self.path == "/api/mkdir":
                return self.send_json(create_folder(req["root"], req.get("prefix", ""), req["name"]))
            if self.path == "/api/write":
                return self.send_json(write_text(req["root"], req["path"], req.get("content", ""),
                                                 bool(req.get("crlf")), bool(req.get("create"))))
            op, root = req["op"], req["root"]
            items = req.get("items", [])
            overwrite = bool(req.get("overwrite"))
            if op == "trash":
                job = start_job(op, op_trash, root, items)
            elif op == "delete":
                job = start_job(op, op_delete, root, items)
            elif op == "move":
                job = start_job(op, op_move, root, items, req["dest"], overwrite)
            elif op == "rename":
                job = start_job(op, op_rename, root, items[0], req["name"], overwrite)
            elif op == "restore":
                job = start_job(op, op_restore, root, items, overwrite)
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
    ap = argparse.ArgumentParser(description="Local web GUI for cloud-files buckets")
    ap.add_argument("path", nargs="?", help="bucket path to open, e.g. nokura://my_bucket/")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()

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
