from typing import Final
import os
from pathlib import Path
from dotenv import load_dotenv

BLSH_HOME: Final = Path.home() / ".blsh"
CONFIG_DIR: Final = BLSH_HOME / "config"
DATA_DIR: Final = BLSH_HOME / "data"
TEMP_DIR: Final = BLSH_HOME / "temp"
LOG_DIR: Final = BLSH_HOME / "logs"
BACKUP_DIR: Final = BLSH_HOME / "backup"

DATA_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

KIS_DEVLP_YAML: Final = CONFIG_DIR / "kis_devlp.yaml"

load_dotenv()
load_dotenv(CONFIG_DIR / ".env")

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

KIS_ENV: Final = os.getenv("KIS_ENV", "demo").lower()  # KIS 모드: "demo" | "real"
TRADE_FLAG: Final = os.environ.get(
    "TRADE_FLAG", "SWING"
).upper()  # 트레이딩 모드: "DAY" | "SWING"

USE_WEBSOCKET: Final = os.getenv("USE_WEBSOCKET", "").lower() in ("1", "true", "yes")

TELEGRAM_BOT_TOKEN: Final = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: Final = os.getenv("TELEGRAM_CHAT_ID")
