import os
import pandas as pd
import requests
import logging
import jinja2 as j2 
import yaml


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
        else:
            logging.error('Missing API key.')

    @staticmethod
    def fx_rate(
            to_symbol:str,
            api_key:str
        )->pd.DataFrame:
        """
        Extracts daily exchange rates.
        - `from_symbol`: The currency symbol from which the exchange occurs.
        - `to_symbol`: The currency symbol into which the exchange occurs.
        """
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")
        
        url = f'https://www.alphavantage.co/query?' \
              f'function=FX_DAILY&' \
              f'from_symbol=USD&' \
              f'to_symbol={to_symbol}&' \
              f'outputsize=full&' \
              f'apikey={api_key}'
            
        if api_key:
            r = requests.get(url)
            if r.status_code == 200:
                try:
                    response_data = r.json()
                    df = pd.DataFrame(response_data['Time Series FX (Daily)']).transpose().reset_index()
                    df = df.rename(columns={ df.columns[0]: "date" })
                    df['to'] = f'{to_symbol}'
                    return df
                except KeyError:
                    logging.error(f'Error extracting exchange rates to {to_symbol} from API')
            else:
                logging.error('Call to API - exchange rates - failed.')

    @staticmethod
    def several_fx_rates(
            currencies: list,
            api_key: str
    ) -> pd.DataFrame:
        df_concat = pd.DataFrame()
        for currency_name in currencies:
            df_extracted = Extract.fx_rate(to_symbol=currency_name, api_key=api_key)
            df_concat = pd.concat([df_concat, df_extracted])
        return df_concat.reset_index().drop(labels=["index"], axis=1)
