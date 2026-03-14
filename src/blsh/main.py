import logging
from blsh.wye.domestic import collector, scanner, simulator

log = logging.getLogger(__name__)


if __name__ == "__main__":
    # collector.collect()

    # 투자 대상 선정
    results, base_date, target_date = scanner.scan("20240702")

    # 투자 시뮬레이션
    simulator.simulate(results, base_date, target_date)

    log.info(
        f"전체 완료: 총 {len(results)}건  기준일={base_date}  목표일={target_date}"
    )
