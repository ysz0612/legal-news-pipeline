import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
env = ROOT / '.env'
if env.exists():
    for line in env.read_text(encoding='utf-8-sig').splitlines():
        if line.strip() and not line.lstrip().startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            os.environ.setdefault(key.strip(), value.strip().strip('\"\''))
DATA = Path(os.environ.get('NEWS_DATA_DIR', str(ROOT / 'data')))
if not DATA.is_absolute():
    DATA = ROOT / DATA


def filters():
    return json.loads((ROOT / 'settings' / 'filters.json').read_text(encoding='utf-8-sig'))
