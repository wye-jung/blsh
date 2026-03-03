from typing import Final
import os
from dotenv import load_dotenv

_USER_HOME: Final = os.path.expanduser("~")
_BLSH_HOME: Final = os.path.join(_USER_HOME, "workspace/blsh")
TEMP_DIR: Final = os.path.join(_BLSH_HOME, ".temp")
CONFIG_DIR: Final = os.path.join(_USER_HOME, ".blsh/config")
KIS_DEVLP_YAML = os.path.join(CONFIG_DIR, "kis_devlp.yaml")

load_dotenv(os.path.join(CONFIG_DIR, ".env"))

DB_USER: Final = os.getenv("DB_USER")
DB_PASSWORD: Final = os.getenv("DB_PASSWORD")
DB_NAME: Final = os.getenv("DB_NAME")
DB_HOST: Final = os.getenv("DB_HOST")
DB_PORT: Final = os.getenv("DB_PORT")
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

KRX_API_KEY: Final = os.getenv("KRX_API_KEY")
DART_API_KEY: Final = os.getenv("DART_API_KEY")

KRX_API_URL: Final = "https://data-dbg.krx.co.kr/svc/apis"

KRX_LOGIN_ID: Final = os.getenv("KRX_LOGIN_ID")
KRX_LOGIN_PW: Final = os.getenv("KRX_LOGIN_PW")
