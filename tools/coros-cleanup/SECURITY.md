# Security And Privacy

This tool uses a real browser session to access COROS Training Hub. It never asks for your COROS password in the terminal.

Important points:

- Login happens in your browser.
- The tool reads COROS cookies from the local browser DevTools protocol.
- Dry-run backup files contain activity metadata and `labelId` values.
- Delete logs contain each attempted `labelId` and API response.
- Do not publish `coros-cleanup-data/`, dry-run backups, or delete logs.
- Use an isolated browser profile created by the tool when possible.

If a delete run is interrupted, do not blindly rerun the same old backup. Run a fresh dry-run and delete from the new backup.
