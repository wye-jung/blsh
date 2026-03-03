import os

import pandas as pd

from _utils import download_and_extract
from blsh.common.env import TEMP_DIR


def get_sector_master_dataframe(base_dir):
    download_and_extract(
        "https://new.real.download.dws.co.kr/common/master/idxcode.mst.zip", base_dir
    )

    file_name = os.path.join(base_dir, "idxcode.mst")
    df = pd.DataFrame(columns=["sec_cd", "sec_nm"])

    ridx = 1
    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            tcode = row[1:5]  # 업종코드 4자리 (맨 앞 1자리 제거)
            # tname = row[3:43].rstrip()  # 업종명
            tname = row[5:43].rstrip()  # 업종명
            df.loc[ridx] = [tcode, tname]
            ridx += 1

    return df


if __name__ == "__main__":
    df = get_sector_master_dataframe(TEMP_DIR)
    print(len(df))
    print(df.head())
    df.to_excel(os.path.join(TEMP_DIR, "sector_code.xlsx"), index=False)
