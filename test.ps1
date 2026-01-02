Write-Host "Creating US region order..." -ForegroundColor Green
$usOrder = @{
    customerId = "customer-001"
    customerRegion = "us"
    items = @(
        @{ productId = "PROD-001"; quantity = 2 }
        @{ productId = "PROD-002"; quantity = 1 }
    )
} | ConvertTo-Json -Depth 3

Invoke-RestMethod -Uri "http://localhost:8080/api/orders" -Method Post -Body $usOrder -ContentType "application/json"

Start-Sleep -Seconds 2

Write-Host "Creating EU region order..." -ForegroundColor Green
$euOrder = @{
    customerId = "customer-002"
    customerRegion = "eu"
    items = @(
        @{ productId = "PROD-003"; quantity = 5 }
    )
} | ConvertTo-Json -Depth 3

Invoke-RestMethod -Uri "http://localhost:8080/api/orders" -Method Post -Body $euOrder -ContentType "application/json"

Start-Sleep -Seconds 2

Write-Host "Creating multiple orders for load testing..." -ForegroundColor Yellow
for ($i = 1; $i -le 10; $i++) {
    $region = if ($i % 2 -eq 0) { "us" } else { "eu" }
    $order = @{
        customerId = "customer-$i"
        customerRegion = $region
        items = @(
            @{ productId = "PROD-00$($i % 5 + 1)"; quantity = $i }
        )
    } | ConvertTo-Json -Depth 3

    Write-Host "Order $i ($region region)..."
    Invoke-RestMethod -Uri "http://localhost:8080/api/orders" -Method Post -Body $order -ContentType "application/json"
    Start-Sleep -Milliseconds 500
}

Write-Host "`nDone! Check Grafana at http://localhost:3000" -ForegroundColor Cyan
Write-Host "Check Jaeger at http://localhost:16686" -ForegroundColor Cyan