@echo off
rem Launch Bucket Browser.
rem Usage: start.bat                    (type a path in the page)
rem        start.bat nokura://my_bucket/
rem Uses the "tem" conda env if present, otherwise whatever "python" is on PATH.
rem Set BB_PYTHON to override, e.g.  set BB_PYTHON=C:\path\to\python.exe
if not defined BB_PYTHON (
  if exist "%LOCALAPPDATA%\anaconda3\envs\tem\python.exe" (
    set "BB_PYTHON=%LOCALAPPDATA%\anaconda3\envs\tem\python.exe"
  ) else (
    set "BB_PYTHON=python"
  )
)
"%BB_PYTHON%" "%~dp0server.py" %*
