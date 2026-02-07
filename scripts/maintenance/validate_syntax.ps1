$targetFile = "c:\Apps\CZ_Digital_Central\diagnostics\check_sql_connection.ps1"
$errors = @()
$tokens = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($targetFile, [ref]$tokens, [ref]$errors)

if ($errors) {
    Write-Host "`nSyntax errors found in $targetFile :" -ForegroundColor Red
    foreach ($err in $errors) {
        Write-Host "Line $($err.Extent.StartLineNumber), Col $($err.Extent.StartColumnNumber): $($err.Message)" -ForegroundColor Yellow
    }
}
else {
    Write-Host "`n✓ No syntax errors found in $targetFile" -ForegroundColor Green
}
