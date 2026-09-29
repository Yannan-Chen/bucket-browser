# Bucket Browser

A local, file-explorer-style web GUI for object storage, built on
[cloud-files](https://github.com/seung-lab/cloud-files). No mounting and no admin setup:
it uses the same credentials cloud-files already uses
(`~/.cloudfiles/secrets`, `~/.cloudvolume/secrets`).

## Run

Double-click `start.bat`, or run:

    start.bat nokura://my_bucket/

This opens http://127.0.0.1:8765/ in your browser. You can type any cloud-files path in
the address box: `nokura://bucket/...`, `gs://bucket/...`, `s3://bucket/...`,
`matrix://...`, `file://C:/some/folder`. Press Ctrl+C in the console window to stop it.

It needs Python 3.9+ with `cloud-files` (`pip install -r requirements.txt`). `start.bat`
uses the `tem` conda env if it exists, otherwise `python` on PATH. You can set `BB_PYTHON`
to point at a specific interpreter, or run it directly:
`python server.py [path] [--port N] [--no-browser]`.

## Controls

| Action | How |
|---|---|
| Open folder / view image | Double-click or Enter (jpg, png, gif, webp, bmp, svg) |
| Image viewer | Left/Right: previous/next, click or 1:1: actual size, Esc: close |
| Open text file | Double-click or Enter (txt, md, json, csv, yaml, py, `info`, ...). Ctrl+S saves, Esc closes. |
| New folder | F7, the **New folder** button, or right-click |
| New text file | Shift+F7, the **New text file** button, or right-click (opens it in the editor) |
| Parent folder | Backspace, the up arrow button, or click a breadcrumb |
| Back / forward | Alt+Left / Alt+Right (browser history works too) |
| Select | Click, Ctrl+click, Shift+click, Ctrl+A, arrow keys (+Shift) |
| Move to trash | Delete (moves to `<bucket>/.Trash/<original path>`) |
| Delete permanently | Ctrl+Delete or Shift+Delete (asks first). Delete inside .Trash is also permanent. |
| Move | Ctrl+X, go to the destination folder, Ctrl+V. Or drag rows onto a folder or breadcrumb. Pasting onto a folder that already exists merges the two (see Notes). |
| Rename | F2 |
| Restore from trash | Right-click in .Trash, then Restore (puts it back at its original path) |
| Copy cloud path | Ctrl+C or right-click, then Copy path |
| Filter / refresh | Ctrl+F / F5 |

## Notes

- **How moves work:** S3-style object storage (including nokura, a Dell ECS system) has no
  rename or "move folder" operation. An object's full path is its name, so moving a
  folder means asking the server to copy each object (S3 `CopyObject`) and then delete
  the original. The server copies the bytes itself, so no data is downloaded to your
  PC. The cost is one request per object:
  - Copies run in 6 worker processes with 16 copies each, over reused connections. A
    single Python process tops out around 300 requests/s; several processes reach the
    storage server's limit (about 1,000 requests/s measured on nokura). Tune this with
    `start.bat --processes N --threads N`. Each worker takes a few seconds to start on
    Windows, so two start when you open a bucket and the rest when you press Ctrl+X.
  - Originals are deleted 1,000 per request. Stock cloud-files deletes nokura objects one
    request at a time, because ECS requires a `Content-MD5` header that newer boto3 no
    longer sends. Bucket Browser adds that header.
  - Listing runs in parallel with copying, so copying starts right away.
  - Moves on `file://` paths are real renames and are instant.
- **Browsing comes first:** while you open folders or view files during a move or delete,
  the background work slows down (2 copies per worker, 1 delete request at a time) so the
  storage server and your PC answer your clicks first. The progress card says "slowed
  while you browse", and full speed resumes about 3 seconds after you stop. Worker
  processes also run at below-normal Windows priority.
- **Merging folders:** pasting a folder where one with the same name already exists
  merges them. Files with different names on either side are always kept. If some files
  have the same name, you're shown them and choose **Replace** (the moved file wins) or
  **Skip** (the file already there is kept, and yours stays in the original folder).
  Enter picks Skip. A single file pasted onto a same-name file asks the same question.
  Renaming onto a name that already exists is refused.
- **Progress and Cancel:** moves, trash and deletes show a progress card (count, rate,
  time left) with a **Cancel** button that works while listing, moving or deleting.
  Cancelling never loses data: each object is deleted from the source only after its
  copy exists. After a cancelled move, some objects are at the destination and the rest
  are still at the source. To finish, paste the rest again: the two parts merge without
  any questions, since no names overlap. Tasks keep running if you close or reload the
  page, and the progress card comes back when you reopen it.
- **Trash vs. permanent delete:** moving a folder to .Trash is a full move (a copy of
  everything). For big folders you don't need back, Ctrl+Delete is much faster: it only
  deletes, 1,000 objects per request.
- **Permissions:** `CopyObject` doesn't copy per-object ACLs. Buckets that are public
  through a bucket policy stay public after a move. A bucket that relies on per-object
  public-read ACLs would lose them on moved objects.
- If a name already exists in .Trash, the new trashed item gets a `~YYYYMMDD-HHMMSS`
  suffix, so nothing in the trash is ever overwritten.
- Moves only work within one bucket.
- Object storage can't hold a truly empty folder, so **New folder** writes a zero-byte
  `name/` marker object (the same convention the AWS and Google Cloud consoles use).
  Moving, trashing or deleting a folder handles these markers too, including empty
  subfolders inside it.
- The text editor opens UTF-8 files up to 5 MB, refuses binary files, and keeps Windows
  (CRLF) line endings if the file had them.
- A folder listing shows at most 20,000 entries.
- The server only listens on 127.0.0.1, and every API call needs a random token that is
  created at startup and embedded in the page. Other websites can't use it to reach
  your buckets.
