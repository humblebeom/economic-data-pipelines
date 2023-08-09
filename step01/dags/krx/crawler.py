from typing import Dict, Tuple
import requests
from copy import copy
import pandas as pd
from io import BytesIO
import warnings
from datetime import datetime


class KRXCrawler:
    def __init__(self, target_date: str):
        """
        :param target_date: "YYYYMMDD"
        """
        self.target_date = target_date
        self.uri = {
            "gen_otp": "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd",
            "download_excel": "http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd",
        }
        self.headers = {
            "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        }
        self.query_params = {
            "trdDd": target_date,
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false",
            "name": "fileDown",
            "url": "dbms/MDC/STAT/standard/MDCSTAT00101",
        }
        self.trillion = 1_000_000_000_000

    def run(self, market_code: str) -> pd.DataFrame:
        """
        시장의 일별시세정보를 반환
        :param market_code: 시장 코드("01": KRX, "02": KOSPI, "03": KOSDAQ)
        :return: df
        """
        form_data = self.request_otp(market_code)
        content = self.request_data(market_code, form_data)
        df = self.make_dataframe(content)

        return df

    def request_otp(self, market_code: str) -> Dict:
        """
        KRX 정보데이터시스템에 일별시세조회를 위한 OTP 생성 요청
        :param market_code: 시장 코드("01": KRX, "02": KOSPI, "03": KOSDAQ)
        :return: form_data
        """
        query_params = copy(self.query_params)
        query_params["idxIndMidclssCd"] = market_code
        url = self.uri.get("gen_otp")
        res = requests.get(url, query_params, headers=self.headers)
        form_data = {"code": res.content}

        return form_data

    def request_data(self, market_code: str, form_data: Dict) -> bytes:
        """
        KRX 정보데이터시스템에 시장의 일별시세정보 요청
        :param market_code: 시장 코드("01": KRX, "02": KOSPI, "03": KOSDAQ)
        :param form_data: KRX 정보데이터시스템 조회를 위한 OTP
        :return: bytes data
        """
        query_params = copy(self.query_params)
        query_params["idxIndMidclssCd"] = market_code
        url = self.uri.get("download_excel")
        res = requests.post(url, form_data, headers=self.headers)

        return res.content

    @staticmethod
    def make_dataframe(content: bytes) -> pd.DataFrame:
        """
        bytes 데이터를 pandas dataframe으로 변환
        :param content: bytes
        :return: df
        """
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            df = pd.read_excel(BytesIO(content), engine="openpyxl")

        return df


if __name__ == "__main__":
    today = "20221121"
    crawler = KRXCrawler(today)
    df_ = crawler.run("02")
    df_.to_csv("/tmp/daily_kospi_index.csv")
    print()

    #            지수명       종가     대비   등락률       시가       고가       저가        거래량           거래대금            상장시가총액
    # 0  코스피 (외국주포함)        -      -     -        -        -        -  544528841  7049427243317  1912994304976186
    # 1          코스피   2419.5 -24.98 -1.02  2446.05  2448.14  2409.36  544303313  7048566723017  1912211045650561
    # 2      코스피 200   314.67  -2.45 -0.77   317.22   317.65   312.99  123416831  5039138952131  1677964380335380
    # 3      코스피 100  2370.53 -18.43 -0.77  2389.17  2391.47  2357.42   62783918  3798865508561  1522365060934830
    # 4       코스피 50  2163.21 -17.22 -0.79  2180.72  2183.03  2151.18   37073287  3083750449511  1303122918899750
