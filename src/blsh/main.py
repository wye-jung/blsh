import logging
from blsh.wye.domestic import collector, scanner, simulator

log = logging.getLogger(__name__)


if __name__ == "__main__":
    # 실적 데이터 수집
    # collector.collect()

    # 투자대상 선정
    candidates, target_date, base_date = scanner.scan("20240702")

    # 투자 시뮬레이션
    simulator.simulate(candidates, target_date)

    # trader.trade(candidates, target_date)

    log.info(
        f"전체 완료: 총 {len(candidates)}건  기준일={base_date}  목표일={target_date}"
    )
