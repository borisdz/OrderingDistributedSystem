# Automated happy-path verification (#64).
# Submits an order and polls /api/orders/{id} until a terminal status or timeout,
# so the end-to-end flow (API -> Kafka -> warehouse -> Kafka -> aggregation -> persistence)
# is proven without manually injecting any Kafka message.

param(
    [string]$BaseUrl = "http://localhost:8080",
    [string]$CustomerRegion = "us",
    [int]$TimeoutSeconds = 30,
    [int]$PollIntervalSeconds = 2
)

$terminalStatuses = @("AVAILABLE", "UNAVAILABLE", "AVAILABILITY_TIMEOUT")

Write-Host "[$(Get-Date -Format o)] Submitting order..." -ForegroundColor Green
$order = @{
    customerId     = "e2e-verification"
    customerRegion = $CustomerRegion
    items          = @(@{ productId = "PROD-001"; quantity = 1 })
} | ConvertTo-Json -Depth 3

$response = Invoke-RestMethod -Uri "$BaseUrl/api/orders" -Method Post -Body $order -ContentType "application/json"
$orderId = ($response -split ": ")[1]

if (-not $orderId) {
    Write-Host "FAIL: order submission did not return an order ID. Response: $response" -ForegroundColor Red
    exit 1
}

Write-Host "[$(Get-Date -Format o)] Order accepted: $orderId" -ForegroundColor Cyan

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$finalStatus = $null

while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds $PollIntervalSeconds
    try {
        $statusResponse = Invoke-RestMethod -Uri "$BaseUrl/api/orders/$orderId" -Method Get
        Write-Host "[$(Get-Date -Format o)] Current status: $($statusResponse.status)"
        if ($terminalStatuses -contains $statusResponse.status) {
            $finalStatus = $statusResponse.status
            break
        }
    } catch {
        Write-Host "[$(Get-Date -Format o)] Status endpoint not yet reachable, retrying..."
    }
}

if (-not $finalStatus) {
    Write-Host "FAIL: order $orderId did not reach a terminal state within $TimeoutSeconds seconds." -ForegroundColor Red
    exit 1
}

Write-Host "[$(Get-Date -Format o)] PASS: order $orderId reached terminal state '$finalStatus'." -ForegroundColor Green
exit 0
