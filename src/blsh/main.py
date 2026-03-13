from blsh.wye.domestic import collector, scanner
from blsh.kis.domestic_stock import domestic_stock_info as info


def main():
    # collector.collect()
    # scanner.scan()
    print(info.get_sector_info())


if __name__ == "__main__":
    main()
