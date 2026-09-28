# =============================================================================
#  sketchstream - one-command runner
#  Memory-Efficient Real-Time Network Anomaly Detection Using Probabilistic
#  Sketches in Distributed Data Streams
#
#  Usage (from anywhere):
#     powershell -ExecutionPolicy Bypass -File run_all.ps1
#     powershell -ExecutionPolicy Bypass -File run_all.ps1 -Quick -NoDashboard
#     powershell -ExecutionPolicy Bypass -File run_all.ps1 -Distributed
#
#  Or just double-click  run_all.bat
#
#  What it does (local, no Docker):
#     1. installs dependencies
#     2. runs the 41 unit tests
#     3. runs the synthetic experiments (CSV + figures)
#     4. runs the real CIC-IDS2017 evaluation (or a self-test if data is absent)
#     5. generates the dashboard stream
#     6. launches the dashboard
#
#  With -Distributed it also:
#     7. starts Kafka + Flink (Docker), produces events, runs the PyFlink job
#        (incremental O(1) state), prints the emitted windows/alerts, then leaves
#        the cluster running.
# =============================================================================

[CmdletBinding()]
param(
    [switch]$Quick,          # smaller/faster experiment sizes
    [switch]$SkipInstall,    # don't pip install
    [switch]$SkipTests,      # don't run pytest
    [switch]$NoDashboard,    # don't launch Streamlit
    [switch]$Distributed     # also run the Kafka + Flink path (needs Docker Desktop)
)

$ErrorActionPreference = 'Continue'
$root = $PSScriptRoot
if (-not $root) { $root = (Get-Location).Path }
Set-Location $root

$script:Failures = @()

function Section([string]$title) {
    Write-Host ""
    Write-Host ("=" * 72) -ForegroundColor DarkCyan
    Write-Host ("  " + $title) -ForegroundColor Cyan
    Write-Host ("=" * 72) -ForegroundColor DarkCyan
}
function Step([string]$msg)  { Write-Host ("  -> " + $msg) -ForegroundColor Gray }
function Ok([string]$msg)    { Write-Host ("  [OK] " + $msg) -ForegroundColor Green }
function Warn([string]$msg)  { Write-Host ("  [!!] " + $msg) -ForegroundColor Yellow }
function Bad([string]$msg)   { Write-Host ("  [XX] " + $msg) -ForegroundColor Red; $script:Failures += $msg }

function Invoke-Py {
    param([string[]]$PyArgs)
    # Send the command's output to the host (not the success stream) so that the
    # function returns ONLY the exit code.
    & python @PyArgs | Out-Host
    return $LASTEXITCODE
}

Write-Host ""
Write-Host "  sketchstream - full project runner" -ForegroundColor White
Write-Host ("  root: " + $root) -ForegroundColor DarkGray

# -----------------------------------------------------------------------------
Section "0. Environment"
$pyVersion = (& python --version 2>&1) -join " "
if ($LASTEXITCODE -ne 0) {
    Bad "Python not found on PATH. Install Python 3.10+ and re-run."
    exit 1
}
Ok "python: $pyVersion"
if ($Distributed) {
    $dockerOk = (& docker info --format '{{.ServerVersion}}' 2>&1)
    if ($LASTEXITCODE -ne 0) {
        Bad "Docker daemon not running. Start Docker Desktop, then re-run with -Distributed."
        exit 1
    }
    Ok "docker: $dockerOk"
}

# -----------------------------------------------------------------------------
if (-not $SkipInstall) {
    Section "1. Install dependencies"
    Step "pip install -r requirements.txt"
    $code = Invoke-Py @('-m', 'pip', 'install', '-q', '-r', 'requirements.txt')
    if ($code -ne 0) { Bad "pip install failed" } else { Ok "dependencies installed" }
} else { Warn "install skipped (-SkipInstall)" }

# -----------------------------------------------------------------------------
if (-not $SkipTests) {
    Section "2. Unit tests (expect 41 passed)"
    $code = Invoke-Py @('-m', 'pytest', '-q')
    if ($code -ne 0) { Bad "tests failed" } else { Ok "all tests passed" }
} else { Warn "tests skipped (-SkipTests)" }

# -----------------------------------------------------------------------------
Section "3. Synthetic experiments"
$expArgs = @('scripts/run_local_experiments.py')
if ($Quick) { $expArgs += '--quick' }
$code = Invoke-Py $expArgs
if ($code -ne 0) { Bad "experiments reported an error" }
else { Ok "results written to results\ (CSV) and figures\ (PNG)" }

# -----------------------------------------------------------------------------
Section "4. CIC-IDS2017 evaluation"
$cicFile = Get-ChildItem -Path 'data\cicids' -Filter '*.csv' -ErrorAction SilentlyContinue |
           Sort-Object Length | Select-Object -First 1
if ($cicFile) {
    Step ("real data: " + $cicFile.Name)
    $code = Invoke-Py @('scripts/run_cicids_evaluation.py', '--cicids', $cicFile.FullName)
    if ($code -ne 0) { Bad "CIC-IDS evaluation failed" }
    else { Ok "real-data numbers written to results\cicids_*.csv" }
} else {
    Warn "no CSV found in data\cicids - running the synthetic-format self-test instead"
    $code = Invoke-Py @('scripts/run_cicids_evaluation.py', '--demo', '--limit', '200000')
    if ($code -ne 0) { Bad "CIC-IDS demo evaluation failed" }
    else { Ok "demo numbers written to results\cicids_*.csv" }
}

# -----------------------------------------------------------------------------
Section "5. Generate dashboard stream"
$code = Invoke-Py @('scripts/demo_stream.py', '--scenario', 'mixed', '--duration', '90', '--base-rate', '2000')
if ($code -ne 0) { Bad "demo streaming failed" } else { Ok "results\stream.jsonl written" }

# -----------------------------------------------------------------------------
if (-not $NoDashboard) {
    Section "6. Launch dashboard"
    Start-Process -FilePath "python" `
        -ArgumentList @('-m', 'streamlit', 'run', 'dashboard/app.py',
                        '--server.headless', 'true', '--server.port', '8501')
    Ok "dashboard starting at http://localhost:8501  (click 'Replay live')"
    Start-Sleep -Seconds 6
} else { Warn "dashboard skipped (-NoDashboard)" }

# -----------------------------------------------------------------------------
if ($Distributed) {
    Section "7. Distributed Kafka + Flink"

    # 7a. ensure the Kafka connector jar exists (needed by the image build; it is
    #     NOT committed because it is a 5 MB binary)
    $jarName = 'flink-sql-connector-kafka-3.4.0-1.20.jar'
    $jarPath = Join-Path 'jars' $jarName
    if (-not (Test-Path $jarPath)) {
        New-Item -ItemType Directory -Force -Path 'jars' | Out-Null
        $jarUrl = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/$jarName"
        Step "downloading Kafka connector jar from Maven Central..."
        try {
            Invoke-WebRequest -UseBasicParsing -Uri $jarUrl -OutFile $jarPath -TimeoutSec 300
            Ok "jar downloaded"
        } catch {
            Bad "could not download the Kafka connector jar; place '$jarName' in jars\ manually"
        }
    } else { Ok "Kafka connector jar present" }

    # 7b. ensure a producer environment that has confluent-kafka
    $prodPy = "python"
    if (Test-Path '.\.venv311\Scripts\python.exe') {
        $prodPy = ".\.venv311\Scripts\python.exe"
        Ok "producer environment (.venv311) present"
    } else {
        Step "no .venv311 - creating one for the Kafka producer..."
        $p311 = "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe"
        if (-not (Test-Path $p311)) { $p311 = "py" }
        if ($p311 -eq "py") { & py -3.11 -m venv .venv311 2>$null }
        else { & $p311 -m venv .venv311 }
        if (Test-Path '.\.venv311\Scripts\python.exe') {
            & .\.venv311\Scripts\python.exe -m pip install -q confluent-kafka
            $prodPy = ".\.venv311\Scripts\python.exe"
            Ok "producer environment ready"
        } else {
            Warn "Python 3.11 not found; using the default python (needs confluent-kafka installed)"
        }
    }

    # 7c. build image only if missing
    $img = (& docker images -q sketchstream-flink:local 2>$null)
    if (-not $img) {
        Step "building Flink+Python image (first run only, several minutes)..."
        & docker compose -f deploy\docker-compose.yml build
        if ($LASTEXITCODE -ne 0) { Bad "docker compose build failed" }
    } else { Ok "Flink image already built" }

    # 7d. start cluster
    Step "starting Kafka + JobManager + 2 TaskManagers..."
    & docker compose -f deploy\docker-compose.yml up -d --scale taskmanager=2 | Out-Null
    if ($LASTEXITCODE -ne 0) { Bad "docker compose up failed" }

    Step "waiting for the Flink cluster..."
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 3
        try {
            $ov = (Invoke-WebRequest -UseBasicParsing 'http://localhost:8082/overview' -TimeoutSec 5).Content
            if ($ov -match '"taskmanagers":2') { $ready = $true; break }
        } catch { }
    }
    if ($ready) { Ok "Flink cluster ready: 2 TaskManagers (UI http://localhost:8082)" }
    else { Warn "cluster did not report 2 TaskManagers in time; continuing anyway" }

    # 7e. produce events
    Step "producing a small anomaly stream into Kafka..."
    & $prodPy scripts\kafka_producer.py --scenario sudden --duration 8 --base-rate 150 --limit 6000
    if ($LASTEXITCODE -ne 0) { Bad "Kafka producer failed (is confluent-kafka installed in .venv311?)" }

    # 7f. submit the incremental PyFlink job
    Step "submitting the PyFlink job (--incremental: O(1) window memory)..."
    $runOut = "/tmp/flink_run.jsonl"
    $submit = 'flink run -d -py /opt/flink/flink_job.py --bootstrap kafka:29092 ' +
              '--from-earliest --incremental --out ' + $runOut +
              ' --window-size 1 --parallelism 2 --warmup 2 --z-threshold 3.0'
    & docker exec sketchstream-jobmanager bash -lc $submit | Out-Null

    Step "waiting for the job to process the stream..."
    Start-Sleep -Seconds 55

    # 7g. show output from whichever TaskManager ran the window operator
    Write-Host ""
    Write-Host "  ---- emitted windows / alerts ----" -ForegroundColor White
    $shown = $false
    foreach ($tm in @('sketchstream-taskmanager-1', 'sketchstream-taskmanager-2')) {
        $out = & docker exec $tm bash -lc "test -s $runOut && cat $runOut" 2>$null
        if ($out) { $out | ForEach-Object { Write-Host ("  " + $_) }; $shown = $true; break }
    }
    if (-not $shown) { Warn "no output yet - the window operator may still be catching up; the job stays running" }
    else { Ok "distributed detection complete (a window with alert=true should appear above)" }

    # 7h. copy evidence out and cancel the job
    Remove-Item -Force 'results\flink_run.jsonl' -ErrorAction SilentlyContinue
    foreach ($tm in @('sketchstream-taskmanager-1', 'sketchstream-taskmanager-2')) {
        & docker cp "${tm}:$runOut" "results\flink_run.jsonl" 2>$null
        if (Test-Path 'results\flink_run.jsonl') { break }
    }
    $jobId = (& docker exec sketchstream-jobmanager bash -lc "flink list -r 2>/dev/null | grep -o ' [0-9a-f]\{32\} ' | head -1" 2>$null)
    if ($jobId) { & docker exec sketchstream-jobmanager bash -lc "flink cancel $($jobId.Trim())" | Out-Null }

    Write-Host ""
    Ok "cluster is still running. Stop it with:  docker compose -f deploy/docker-compose.yml down"
}

# -----------------------------------------------------------------------------
Section "Summary"
if ($script:Failures.Count -eq 0) {
    Ok "Everything completed successfully."
    Write-Host ""
    Write-Host "  Local artifacts:" -ForegroundColor White
    Write-Host "     results\   -> experiment + evaluation CSVs (and stream.jsonl)"
    Write-Host "     figures\   -> plots (PNG)"
    if (-not $NoDashboard) { Write-Host "     dashboard  -> http://localhost:8501" }
    if ($Distributed)     { Write-Host "     Flink UI   -> http://localhost:8082" }
} else {
    Write-Host "  These steps reported problems:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host ("   - " + $_) -ForegroundColor Red }
    exit 1
}
