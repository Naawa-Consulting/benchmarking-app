param(
  [string]$WebRoot = "C:\Users\sebmo\OneDrive - Awsaan Consulting SA de CV\Naawa\2026\nw\Product\benchmarking-app\apps\web"
)

Set-Location $WebRoot
npm install
npm run dev
