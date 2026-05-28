# generate_traffic.ps1
# LK-11 Observability Traffic Generator
# Detects scaling mode and routes traffic correctly.

$url = "http://localhost:8000/predict"
Write-Host "Testing connection to $url..."

$useDockerExec = $false
try {
    $test = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2
    Write-Host "Host port 8000 is open. Generating traffic natively..."
} catch {
    Write-Host "Host port 8000 is closed (expected if running in scaled mode without ports mapped)."
    Write-Host "Switching to internal Docker network routing via Streamlit container..."
    $useDockerExec = $true
}

Write-Host "Sending 30 POST requests..."

if ($useDockerExec) {
    # Mount the python script into a temporary container on the mlops-net network to send traffic
    docker compose run --rm -v ${PWD}/monitoring/scripts:/scripts --entrypoint python streamlit /scripts/generate_traffic.py http://model-api:8000/predict
} else {
    python .\monitoring\scripts\generate_traffic.py $url
}

Write-Host "Traffic generation complete. Check Grafana dashboards."
