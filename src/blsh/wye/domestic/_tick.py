"""KRX 호가 단위 보정 유틸"""


def tick_size(price: float) -> int:
    if price < 1_000:
        return 1
    elif price < 5_000:
        return 5
    elif price < 10_000:
        return 10
    elif price < 50_000:
        return 50
    elif price < 100_000:
        return 100
    elif price < 500_000:
        return 500
    else:
        return 1_000


def floor_tick(price: float) -> int:
    """가격을 호가 단위 이하로 내림 (SL 등 하한 기준에 사용)"""
    tick = tick_size(price)
    return int(price) // tick * tick


def ceil_tick(price: float) -> int:
    """가격을 호가 단위 이상으로 올림 (TP 등 상한 기준에 사용)"""
    tick = tick_size(price)
    floored = int(price) // tick * tick
    result = floored if floored >= price else floored + tick
    # 올림 결과가 더 높은 tick 구간으로 넘어간 경우 재보정
    final_tick = tick_size(result)
    if result % final_tick != 0:
        result = (result // final_tick + 1) * final_tick
    return result
