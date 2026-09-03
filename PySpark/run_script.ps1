<#
.SYNOPSIS
    Activates the pyspark-env conda environment and runs a chosen PySpark example script.

.DESCRIPTION
    Lists every .py file in this folder, lets you pick one by number, and runs it
    inside the pyspark-env conda environment (Python 3.11 + OpenJDK 17 + PySpark 4.2).

.USAGE
    powershell -File .\run_script.ps1
#>

$ErrorActionPreference = "Stop"

# --- Locate conda.exe (works even if 'conda init' was never run for this shell) ---
$condaCandidates = @(
    "$env:USERPROFILE\miniconda3\Scripts\conda.exe",
    "$env:USERPROFILE\anaconda3\Scripts\conda.exe",
    "$env:LOCALAPPDATA\miniconda3\Scripts\conda.exe",
    "C:\ProgramData\miniconda3\Scripts\conda.exe"
)
$condaExe = $condaCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $condaExe) {
    Write-Error "Could not find conda.exe. Install Miniconda first (see script_run.md)."
    exit 1
}

# --- Hook conda into this session and activate the environment ---
(& $condaExe "shell.powershell" "hook") | Out-String | Invoke-Expression
conda activate pyspark-env

# --- Safety net: re-apply env vars in case this env wasn't created via environment.yml
#     with the extra one-time setup (see script_run.md) ---
if (-not $env:PYSPARK_PYTHON) {
    $env:PYSPARK_PYTHON = (Get-Command python).Source
    $env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
}
$hadoopHome = Join-Path $env:CONDA_PREFIX "hadoop"
if (-not $env:HADOOP_HOME -and (Test-Path $hadoopHome)) {
    $env:HADOOP_HOME = $hadoopHome
    $env:PATH = "$hadoopHome\bin;$env:PATH"
}

# --- List available scripts in this folder ---
$scripts = Get-ChildItem -Path $PSScriptRoot -Filter "*.py" | Sort-Object Name
if ($scripts.Count -eq 0) {
    Write-Error "No .py scripts found in $PSScriptRoot"
    exit 1
}

Write-Host ""
Write-Host "PySpark scripts available:" -ForegroundColor Cyan
for ($i = 0; $i -lt $scripts.Count; $i++) {
    Write-Host ("  [{0}] {1}" -f ($i + 1), $scripts[$i].Name)
}
Write-Host ""

$selection = Read-Host "Enter the number of the script to run"
$index = 0
if (-not [int]::TryParse($selection, [ref]$index) -or $index -lt 1 -or $index -gt $scripts.Count) {
    Write-Error "Invalid selection: $selection"
    exit 1
}

$target = $scripts[$index - 1]
Write-Host ""
Write-Host "Running $($target.Name) in environment '$env:CONDA_DEFAULT_ENV'..." -ForegroundColor Green
Write-Host ""

python $target.FullName
