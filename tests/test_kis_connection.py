# test_kis_connection.py
# KIS Open API 연결 테스트 및 기본 정보 확인 스크립트
try:
    from blsh.kis.kis_auth import auth, getTREnv, getEnv, read_token
    import blsh.kis.kis_auth as kis_auth

    # 설정 파일 확인
    print("설정 파일 확인 중...")
    cfg = getEnv()
    print(f"앱키: {cfg.get('my_app', 'None')[:10]}...")
    print(f"서버 URL: {cfg.get('prod', 'None')}")

    # 인증 토큰 발급 테스트
    print("토큰 발급 시도 중...")
    try:
        # 디버그 모드 활성화
        kis_auth._DEBUG = True

        auth(svr="vps")  # 모의투자 토큰 발급 및 저장
        print("토큰 발급 완료")

        # 토큰이 제대로 설정되지 않은 경우 수동으로 설정
        env = getTREnv()
        if not env.my_token:
            print("토큰이 환경에 설정되지 않음. 저장된 토큰을 확인합니다...")
            saved_token = read_token()
            if saved_token:
                print("저장된 토큰을 찾았습니다. 환경에 설정합니다...")
                # 토큰을 직접 설정
                kis_auth._TRENV = kis_auth._TRENV._replace(my_token=saved_token)
                kis_auth._base_headers["authorization"] = f"Bearer {saved_token}"
                print("토큰 설정 완료")
            else:
                print("저장된 토큰도 없습니다.")

    except Exception as auth_error:
        print(f"토큰 발급 중 오류: {auth_error}")
        import traceback

        traceback.print_exc()

    # 환경 정보 확인
    env = getTREnv()

    if hasattr(env, "my_token") and env.my_token:
        print("✅ API 연결 성공!")
        print(f"토큰 앞 10자리: {env.my_token[:10]}...")
        print(f"계좌번호: {env.my_acct}")
        print(f"서버: {'모의투자' if env.my_url.find('vts') > 0 else '실전투자'}")
    else:
        print("❌ API 연결 실패 - 토큰이 없습니다")
        print(f"토큰 속성 존재: {hasattr(env, 'my_token')}")
        if hasattr(env, "my_token"):
            print(f"토큰 값: {env.my_token}")
            print(f"토큰 길이: {len(env.my_token) if env.my_token else 0}")

except Exception as e:
    print(f"❌ 오류 발생: {e}")
    print("devlp.yaml 파일 경로와 설정을 확인해주세요")
