from pandas import DataFrame
from pykrx.website.krx.krxio import KrxWebIo


class 전종목기본정보(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT01901"

    def fetch(self, mktId: str) -> DataFrame:
        """[12005] 전종목 기본정보

        Args:
            mktId (str): 조회 시장 (STK/KSQ/KNX/ALL)

        Returns:
            DataFrame: 전종목의 기본 정보
               ISU_CD ISU_SRT_CD      ISU_NM ISU_ABBRV  ... SECT_TP_NM KIND_STKCERT_TP_NM PARVAL    LIST_SHRS
               0  KR7095570008     095570   AJ네트웍스보통주    AJ네트웍스  ...                           보통주  1,000   45,252,759
               1  KR7006840003     006840    AK홀딩스보통주     AK홀딩스  ...                           보통주  5,000   13,247,561
               2  KR7282330000     282330   BGF리테일보통주    BGF리테일  ...                           보통주  1,000   17,283,906
               3  KR7027410000     027410      BGF보통주       BGF  ...                           보통주  1,000   95,716,791
               4  KR7138930003     138930  BNK금융지주보통주   BNK금융지주  ...                           보통주  5,000  310,327,033
           Columns:
               ['ISU_CD', 'ISU_SRT_CD', 'ISU_NM', 'ISU_ABBRV', 'ISU_ENG_NM', 'LIST_DD',
               'MKT_TP_NM', 'SECUGRP_NM', 'SECT_TP_NM', 'KIND_STKCERT_TP_NM', 'PARVAL',
               'LIST_SHRS']
               ['표준코드', '단축코드', '한글종목명', '한글종목약명', '영문종목명', '상장일',
               '시장구분', '증권구분', '소속부', '주식종류', '액면가',
               '상장주식수']

        """
        result = self.read(mktId=mktId)
        return DataFrame(result["OutBlock_1"])


class 자기주식제외시가총액(KrxWebIo):
    @property
    def bld(self):
        return "dbms/MDC/STAT/standard/MDCSTAT18801"

    def fetch(self, trdDd: str, mktId: str) -> DataFrame:
        """[12029] 자기주식제외시가총액

        Args:
            trdDd     (str): 조회 일자 (YYMMDD)
            mktId     (str): 조회 시장 (STK/KSQ/KNX/ALL)

        Returns:
            DataFrame:
                    ISU_SRT_CD        ISU_CD ISU_ABBRV   NON_TRSTK_MKTCAP TDD_CLSPRC NON_TRSTK_LIST_SHRS    LIST_SHRS TRSTK_SHRS ACNTCLS_YYMM
                0       095570  KR7095570008    AJ네트웍스    221,965,208,000      4,960          44,751,050   45,252,759    501,709      2025-09
                1       006840  KR7006840003     AK홀딩스    105,016,234,750      8,050          13,045,495   13,247,561    202,066      2025-09
                2       027410  KR7027410000       BGF    393,841,572,090      4,115          95,708,766   95,716,791      8,025      2025-09
                3       282330  KR7282330000    BGF리테일  1,986,626,725,000    115,000          17,275,015   17,283,906      8,891      2025-09
                4       138930  KR7138930003   BNK금융지주  5,671,094,947,860     18,420         307,877,033  310,327,033  2,450,000      2025-09

                NON_TRSTK_MKTCAP 자기주식 제외 시가총액(AxB)
                TDD_CLSPRC 종가(A)
                NON_TRSTK_LIST_SHRS 자기주식 제외 발행주식수(B=C-D)
                LIST_SHRS 총발행주식수(C)
                TRSTK_SHRS 자기주식수(D)
                ACNTCLS_YYMM 자기주식 반영시점

        """
        result = self.read(mktId=mktId, trdDd=trdDd)
        return DataFrame(result["output"])


if __name__ == "__main__":
    from wye.blsh.krx.krx_auth import login

    login()
    print(전종목기본정보().fetch("STK"))
