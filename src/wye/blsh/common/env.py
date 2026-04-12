from typing import Final
import os
from pathlib import Path
from dotenv import load_dotenv

BLSH_HOME: Final = Path.home() / ".blsh"
CONFIG_DIR: Final = BLSH_HOME / "config"
CACHE_DIR: Final = BLSH_HOME / "cache"
TEMP_DIR: Final = BLSH_HOME / "temp"

KIS_DEVLP_YAML: Final = CONFIG_DIR / "kis_devlp.yaml"

load_dotenv()
load_dotenv(CONFIG_DIR / ".env")

KIS_ENV: Final = os.getenv("KIS_ENV", "demo").lower()  # KIS 모드: "demo" | "real"

DATA_DIR: Final = BLSH_HOME / KIS_ENV / "data"
LOG_DIR: Final = BLSH_HOME / KIS_ENV / "logs"
BACKUP_DIR: Final = BLSH_HOME / KIS_ENV / "backup"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

KIS_APP_KEY: Final = os.getenv("KIS_APP_KEY")
KIS_APP_SECRET: Final = os.getenv("KIS_APP_SECRET")

DB_USER: Final = os.getenv("DB_USER")
DB_PASSWORD: Final = os.getenv("DB_PASSWORD")
DB_NAME: Final = os.getenv("DB_NAME")
DB_HOST: Final = os.getenv("DB_HOST")
DB_PORT: Final = os.getenv("DB_PORT")
DB_URL: Final = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# DART_API_KEY: Final = os.getenv("DART_API_KEY")
# KRX_API_KEY: Final = os.getenv("KRX_API_KEY")
# KRX_API_URL: Final = "https://data-dbg.krx.co.kr/svc/apis"

KRX_LOGIN_ID: Final = os.getenv("KRX_LOGIN_ID")
KRX_LOGIN_PW: Final = os.getenv("KRX_LOGIN_PW")

USE_WEBSOCKET: Final = os.getenv("USE_WEBSOCKET", "").lower() in ("1", "true", "yes")
SCAN_ETF: Final = os.getenv("SCAN_ETF", "").lower() in ("1", "true", "yes")

_kis_rate_limit_cps_raw = os.getenv("KIS_RATE_LIMIT_CPS", "").strip()
KIS_RATE_LIMIT_CPS: Final = int(_kis_rate_limit_cps_raw) if _kis_rate_limit_cps_raw else None

_missing = [
    k
    for k, v in {
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_NAME": DB_NAME,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        "KIS_APP_KEY": KIS_APP_KEY,
        "KIS_APP_SECRET": KIS_APP_SECRET,
    }.items()
    if not v
]
if _missing:
    raise RuntimeError(f"필수 환경변수 누락: {', '.join(_missing)}")

TELEGRAM_BOT_TOKEN: Final = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: Final = os.getenv("TELEGRAM_CHAT_ID")
