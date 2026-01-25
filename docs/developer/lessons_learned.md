# Lessons Learned

## Windows Administration

### Deleting Non-Empty Directories
The standard `rmdir` command often fails on Windows with "Permission Denied" or "Directory not empty" errors when deleting complex folders (especially those containing Python venvs or git repos).

**Best Practice:**
Use PowerShell's `Remove-Item` with `-Recurse` and `-Force` flags:

```powershell
Remove-Item -Path "path\to\directory" -Recurse -Force
```

Do not use `rmdir` or `del` for recursive directory deletion in automation scripts or one-off commands.
