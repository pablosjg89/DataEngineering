# Running the PySpark Examples

This folder includes a `pyspark-env` conda environment and a picker script
(`run_script.ps1`) so you can run any example without remembering setup steps.

## Quick start (environment already set up)

From this folder (or anywhere, it uses its own path):

```powershell
powershell -File .\run_script.ps1
```

It activates `pyspark-env` and lists every `.py` file in this folder — enter
the number of the one you want and it runs.

## Setting up the environment from scratch

1. **Install Miniconda** (if not already installed):
   ```powershell
   winget install -e --id Anaconda.Miniconda3
   ```
   Close and reopen your terminal afterward.

2. **Create the environment** from `environment.yml`:
   ```powershell
   conda env create -f environment.yml
   ```

3. **One-time extra setup** — required on Windows, not captured by
   `environment.yml` because it's machine-specific config, not a package:

   - **`PYSPARK_PYTHON`** — without this, Spark's worker subprocesses run a
     bare `python` command, which on Windows resolves to the Microsoft Store
     Python stub instead of the conda env's interpreter, and jobs hang/fail
     with `Python worker failed to connect back`.

     ```powershell
     conda env config vars set -n pyspark-env `
       PYSPARK_PYTHON="$env:USERPROFILE\miniconda3\envs\pyspark-env\python.exe" `
       PYSPARK_DRIVER_PYTHON="$env:USERPROFILE\miniconda3\envs\pyspark-env\python.exe"
     ```

   - **`winutils.exe` / `hadoop.dll` + `HADOOP_HOME`** — required for any
     file writes (CSV/Parquet/JSON output), otherwise you'll get
     `HADOOP_HOME and hadoop.home.dir are unset` errors on `mkdir`/`chmod`.
     Download a Hadoop 3.3.x/3.4.x build from
     [cdarlint/winutils](https://github.com/cdarlint/winutils) (unofficial
     community binaries — not part of the official Apache Hadoop release)
     into `%CONDA_PREFIX%\hadoop\bin\`, then:

     ```powershell
     conda env config vars set -n pyspark-env `
       HADOOP_HOME="$env:USERPROFILE\miniconda3\envs\pyspark-env\hadoop"
     ```

     `HADOOP_HOME\bin` also needs to be on `PATH` for `hadoop.dll` to load —
     `run_script.ps1` adds this automatically as a fallback even if you skip
     the activate.d hook.

4. **Reactivate** so the new vars take effect:
   ```powershell
   conda deactivate
   conda activate pyspark-env
   ```

## Why OpenJDK 17, not 11 or 8

PySpark 4.2.0 requires Java 17+. Java 11 fails at startup with
`UnsupportedClassVersionError` when launching `org.apache.spark.launcher.Main`.

## Running a single script manually (without the picker)

```powershell
conda activate pyspark-env
python .\01_basic_setup.py
```

## VS Code integration

To make the integrated terminal and Run button use this environment by
default: `Ctrl+Shift+P` → **Python: Select Interpreter** → pick
`pyspark-env` (`...\miniconda3\envs\pyspark-env\python.exe`).

If `conda activate` fails in a fresh terminal with a script-execution error,
run once (per user account, no admin needed):

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```
