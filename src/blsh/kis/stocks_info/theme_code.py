import os

import pandas as pd

from _utils import download_and_extract
from blsh.common.env import TEMP_DIR


def get_theme_master_dataframe(base_dir):
    download_and_extract(
        "https://new.real.download.dws.co.kr/common/master/theme_code.mst.zip", base_dir
    )

    file_name = os.path.join(base_dir, "theme_code.mst")
    df = pd.DataFrame(columns=["theme_cd", "theme_nm", "isu_srt_cd"])

    ridx = 1
    with open(file_name, mode="r", encoding="cp949") as f:
        for row in f:
            tcode = row[0:3]  # 테마코드
            jcode = row[-10:].rstrip()  # 테마명
            tname = row[3:-10].rstrip()  # 종목코드
            df.loc[ridx] = [tcode, tname, jcode]
            # print(df.loc[ridx])  # 파일 작성중인 것을 확인할 수 있음
            ridx += 1

    return df


if __name__ == "__main__":
    df = get_theme_master_dataframe(TEMP_DIR)
    print(len(df))
    print(df.head())
    df.to_excel(os.path.join(TEMP_DIR, "theme_code.xlsx"), index=False)
