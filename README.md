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
| Move | Ctrl+X, go to the destination folder, Ctrl+V. Or drag rows onto a folder or breadcrumb. |
| Rename | F2 |
| Restore from trash | Right-click in .Trash, then Restore (puts it back at its original path) |
| Copy cloud path | Ctrl+C or right-click, then Copy path |
| Filter / refresh | Ctrl+F / F5 |

## Notes

- Object storage has no real folders, so moving a folder means copying and deleting every
  object under it. On S3/GCS the copy is done server-side, and progress shows in the
  status bar. Moving a folder with millions of objects will take a while.
- If a destination already exists, you are asked before anything is overwritten. If a
  name already exists in .Trash, the new trashed item gets a `~YYYYMMDD-HHMMSS` suffix.
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
