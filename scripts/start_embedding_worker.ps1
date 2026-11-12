$ErrorActionPreference = 'Continue'
$ProjectRoot = 'E:\law_project\backend'
$Python = Join-Path $ProjectRoot '.venv-embedding\Scripts\python.exe'
$LogDir = Join-Path $ProjectRoot 'logs'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $ProjectRoot
$env:HF_HOME = 'E:\ai_cache\huggingface'
$env:HUGGINGFACE_HUB_CACHE = 'E:\ai_cache\huggingface\hub'
$env:TORCH_HOME = 'E:\ai_cache\torch'
$env:UV_CACHE_DIR = 'E:\ai_cache\uv'
$env:HF_HUB_OFFLINE = '1'
$env:TRANSFORMERS_OFFLINE = '1'
$env:PYTHONPATH = $ProjectRoot
$env:PYTHONUNBUFFERED = '1'
$env:EMBEDDING_DEVICE = 'cpu'
$env:TOKENIZERS_PARALLELISM = 'false'
$LogPath = Join-Path $LogDir 'embedding-worker.log'
& $Python 'scripts\embedding_worker.py' >> $LogPath 2>&1
