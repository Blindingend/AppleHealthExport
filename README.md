# AppleHealthExport

Tools for converting Apple Health exports and managing related workout data cleanup workflows.

## Apple Health Export Conversion

The main conversion script is:

```bash
python convert_health.py
```

It converts Apple Health export data into workout formats such as `.fit` and `.tcx` for upload to other platforms.

## COROS Training Hub Cleanup

The COROS cleanup helper lives in:

```text
tools/coros-cleanup/
```

It is a dry-run-first CLI for cleaning old or duplicated COROS Training Hub activities. It opens a browser for login, reads activity history through COROS web APIs, writes a full backup before deletion, and requires an exact confirmation phrase before sending delete requests.

Start the guided flow:

```bash
cd tools/coros-cleanup
node coros-cleanup.js
```

See [tools/coros-cleanup/README.md](tools/coros-cleanup/README.md) for full usage and safety notes.

## Privacy

Do not commit raw Apple Health exports, generated workout files, COROS dry-run backups, delete logs, or browser profile data. The repository `.gitignore` excludes the common generated paths and file types.
