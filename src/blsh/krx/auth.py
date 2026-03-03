import requests
from pykrx.website.comm import webio
from blsh.common.env import KRX_LOGIN_ID, KRX_LOGIN_PW

# 1. 공유 세션 생성 및 pykrx에 주입
_session = requests.Session()


def _session_post_read(self, **params):
    return _session.post(self.url, headers=self.headers, data=params)


def _session_get_read(self, **params):
    return _session.get(self.url, headers=self.headers, params=params)


webio.Post.read = _session_post_read
webio.Get.read = _session_get_read


def login_krx():
    """
    KRX data.krx.co.kr 로그인 후 세션 쿠키(JSESSIONID)를 갱신합니다.

    로그인 흐름:
    1. GET MDCCOMS001.cmd  → 초기 JSESSIONID 발급
    2. GET login.jsp       → iframe 세션 초기화
    3. POST MDCCOMS001D1.cmd → 실제 로그인
    4. CD011(중복 로그인) → skipDup=Y 추가 후 재전송
    """
    _LOGIN_PAGE = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
    _LOGIN_JSP = (
        "https://data.krx.co.kr/contents/MDC/COMS/client/view/login.jsp?site=mdc"
    )
    _LOGIN_URL = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D1.cmd"
    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    # 초기 세션 발급
    _session.get(_LOGIN_PAGE, headers={"User-Agent": _UA}, timeout=15)
    _session.get(
        _LOGIN_JSP, headers={"User-Agent": _UA, "Referer": _LOGIN_PAGE}, timeout=15
    )

    payload = {
        "mbrNm": "",
        "telNo": "",
        "di": "",
        "certType": "",
        "mbrId": KRX_LOGIN_ID,
        "pw": KRX_LOGIN_PW,
    }
    headers = {"User-Agent": _UA, "Referer": _LOGIN_PAGE}

    # 로그인 POST
    resp = _session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
    data = resp.json()
    error_code = data.get("_error_code", "")

    # CD011 중복 로그인 처리
    if error_code == "CD011":
        payload["skipDup"] = "Y"
        resp = _session.post(_LOGIN_URL, data=payload, headers=headers, timeout=15)
        data = resp.json()
        error_code = data.get("_error_code", "")

    if error_code != "CD001":  # CD001 = 정상
        raise ValueError(f"Login failed with error code: {error_code}")
