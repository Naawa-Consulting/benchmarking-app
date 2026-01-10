param(
  [string]$ApiBaseUrl = "http://localhost:8000"
)

$uri = "$ApiBaseUrl/demo/seed"

try {
  $response = Invoke-RestMethod -Method Post -Uri $uri
  Write-Host "Seeded demo data:" -ForegroundColor Green
  $response | ConvertTo-Json -Depth 4 | Write-Host
} catch {
  Write-Error "Failed to seed demo data. Ensure the API is running."
  throw
}
