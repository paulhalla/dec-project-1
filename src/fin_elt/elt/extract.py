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
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

        url = f'https://www.alphavantage.co/query?' \
              f'function=TREASURY_YIELD&' \
              f'interval={interval}&' \
              f'maturity={maturity}&' \
              f'apikey={api_key}'

        if api_key:
            r = requests.get(url)
            if r.status_code == 200:
                try:
                    data = r.json()
                    df = pd.json_normalize(data['data'])
                    return df
                except KeyError:
                    logging.error(f'Error extracting {interval} {maturity} treasury yields from API')
            else:
                logging.error('Call to API - treasury yields - failed.')


