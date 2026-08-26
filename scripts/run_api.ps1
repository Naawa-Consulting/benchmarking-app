param(
  [string]$ApiRoot = "C:\Users\sebmo\OneDrive - Awsaan Consulting SA de CV\Naawa\product\bbs\services\api"
)

Set-Location $ApiRoot
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload
