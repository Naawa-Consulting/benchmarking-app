param(
  [string]$WebRoot = "C:\Users\sebmo\OneDrive - Awsaan Consulting SA de CV\Naawa\product\bbs\apps\web"
)

Set-Location $WebRoot
npm install
npm run dev
