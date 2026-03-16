import logging
from blsh.wye.domestic import collector, scanner, trader, simulator

logging.basicConfig(
    level=logging.INFO,
    # format="%(asctime)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s",
    format="%(asctime)s [%(levelname)s] %(message)s",
    # datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


if __name__ == "__main__":
    # 실적 데이터 수집
    collector.collect()

    # 투자대상 선정
    candidates, target_date, base_date = scanner.scan()

    log.info(
        f"투자 대상: 총 {len(candidates)}건  선정 기준일={base_date}  매수 목표일={target_date}"
    )

    # 투자 시뮬레이션
    # simulator.simulate(candidates, target_date)
    trader.run()
