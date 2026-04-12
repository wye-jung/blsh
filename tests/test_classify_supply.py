"""classify_supply 동치성 단위 테스트.

scanner.classify_supply는 두 경로로 호출된다:
  (a) legacy:  classify_supply([d-4, d-3, d-2, d-1, d-0])  — 마지막 원소가 today
  (b) override: classify_supply([d-4, d-3, d-2, d-1], today_override=d-0)
                 — qty_list 전체가 완전일 history, override가 today

두 호출은 동일 데이터에 대해 동일 결과를 반환해야 한다.
또한 각 수급 분류(TRN/C3/1/None)의 경계 동작도 함께 검증한다.

실행: uv run python tests/test_classify_supply.py
"""
from wye.blsh.domestic.scanner import classify_supply


def _eq(label, a, b):
    assert a == b, f"FAIL {label}: legacy={a} override={b}"
    print(f"  OK {label}: {a}")


def test_equivalence():
    """legacy 경로와 today_override 경로가 동일 결과를 내는지 검증."""
    print("[test_equivalence] legacy vs today_override 동치성")

    cases = [
        ("TRN: 매도3일 → 매수전환", [-10, -5, -3, 20]),
        ("C3: 3일 연속 매수", [5, 10, 15, 20]),
        ("C3: 4일 연속 매수", [1, 5, 10, 15, 20]),
        ("TRN: 매도/0 3일 → 매수전환", [-5, -3, 0, 20]),
        ("1: 전일 매도 + 오늘 매수 (history 내 양수 존재)", [5, 10, -5, 20]),
        ("None: 오늘 매도", [5, 10, 15, -5]),
        ("None: 오늘 0", [5, 10, 15, 0]),
    ]

    for label, full in cases:
        legacy = classify_supply(full)
        override = classify_supply(full[:-1], today_override=full[-1])
        _eq(label, legacy, override)


def test_boundaries():
    """경계 조건: 원소 개수 부족, 빈 리스트 등."""
    print("[test_boundaries] 경계 조건")

    # legacy: 2개 미만 → None
    assert classify_supply([]) == (None, 0), "legacy 빈 리스트 → None"
    assert classify_supply([5]) == (None, 0), "legacy 1원소 → None (2 미만)"
    print("  OK legacy 빈/1원소 → None")

    # override: 빈 리스트 → None (today_override 있어도 history 필요)
    assert classify_supply([], today_override=10) == (None, 0), \
        "override 빈 history → None"
    print("  OK override 빈 history → None")

    # override: 1원소 history + 양수 today → "1" (consec=1, C3 미달)
    result = classify_supply([-5], today_override=10)
    assert result[0] == "1", f"override 1원소 매도 → '1': {result}"
    print(f"  OK override 1원소 매도 → {result}")

    # 연속성 계산: TRN은 history 2개 이상 전부 매도일 때만
    assert classify_supply([-5, 10]) == (None, 0) or \
           classify_supply([-5, 10])[0] == "1", \
        "history 1개 매도 → TRN 아님"
    # len(history)=1은 TRN 조건(>=2) 미달이므로 consec 경로로 빠진다
    result = classify_supply([-5, 10])
    assert result[0] == "1", f"history 1개(매도) + today 매수 → '1': {result}"
    print(f"  OK history 1개 매도 + today 매수 → {result}")


def test_trn_vs_c3():
    """TRN과 C3 분류가 정확히 분기되는지."""
    print("[test_trn_vs_c3] TRN/C3 분기")

    # TRN: 직전 전부 <=0 + today > 0 (history >= 2개)
    assert classify_supply([-5, -3, 10])[0] == "TRN"
    assert classify_supply([0, -3, 10])[0] == "TRN"
    assert classify_supply([-5, 0, 10])[0] == "TRN"
    print("  OK TRN: 직전 전부 매도/0 + today 매수")

    # C3: 3일 이상 연속 매수 (today 포함)
    assert classify_supply([-10, 5, 10, 15])[0] == "C3", "consec=3"
    assert classify_supply([5, 10, 15, 20])[0] == "C3", "consec=4"
    print("  OK C3: 3일 이상 연속 매수")

    # "1": 조건 미달 (연속 2일 매수)
    result = classify_supply([-10, -5, 10, 15])
    assert result[0] == "1", f"consec=2 → '1': {result}"
    print(f"  OK 1: consec=2 → {result}")


def main():
    test_equivalence()
    test_boundaries()
    test_trn_vs_c3()
    print("\n✅ classify_supply 단위 테스트 전부 통과")


if __name__ == "__main__":
    main()
