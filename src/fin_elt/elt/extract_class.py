import os
import pandas as pd
import requests
import logging


class Extract:

    @staticmethod
    def treasury_yields(
            interval: str,
            maturity: str,
            api_key: str
    ) -> pd.DataFrame:
        """
        Function to extract historical US Treasury Yields (including current day)

        :param interval: granularity of data (daily, weekly, monthly)
        :param maturity: type of bond (3month, 2year, 5year, 7year, 10year, 30year)
        :param api_key: api key to access Alpha Vantage API
        :return: Pandas Dataframe
        """

        url = f'https://www.alphavantage.co/query?function=TREASURY_YIELD&'f'interval={interval}&'f'maturity={maturity}&'f'apikey={api_key}'

        if api_key:
            logging.info(f'Extracting {interval} {maturity} treasury yields from API.')
            r = requests.get(url)
            if r.status_code == 200:
                try:
                    data = r.json()
                    df = pd.json_normalize(data['data'])
                    return df
                except Exception as e:
                    logging.error(f'Error extracting {interval} {maturity} treasury yields from API ({e})')
            else:
                logging.error('Call to API - treasury yields - failed.')


    @staticmethod
    def multiple_maturities(
            api_key: str,
            interval: str = 'daily'
    ) -> tuple:
        """

        :param api_key:
        :param interval:
        :return:
        """
        options = [
            '3month',
            '2year',
            '5year',
            '7year',
            '10year',
            '30year'
        ]
        for option in options:
            df = Extract.treasury_yields(interval=interval, maturity=option, api_key=api_key)
            if len(df) > 0:
                yield option, df


# TEST
api_key = os.environ.get('AV_API_KEY')
for i in Extract.multiple_maturities(api_key):
    print(i[0], len(i[1]))
