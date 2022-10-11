import os
import logging
import yaml
from database.postgres import PostgresDB
from fin_elt.elt.extract import Extract
from fin_elt.elt.load import Load
import time


def pipeline() -> bool:
    logging.basicConfig(format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)

    #
    # CONFIG
    #
    # Get Alpha Vantage API KEY from environment variable
    api_key = os.environ.get('api_key')

    # Load config data
    with open(f"fin_elt/config.yaml") as stream:
        config = yaml.safe_load(stream)

    #
    # EXTRACT
    #
    logger.info("Commencing extraction")

    # Extract treasury yields - for each maturity
    treasury_data = {}
    for maturity in config['extract']['treasury_yield']['options']:
        logger.info(f"Extracting: {maturity} treasury yield API data")
        treasury_data[maturity] = Extract.treasury_yields(interval='daily', maturity=maturity, api_key=api_key)

    logger.info("Waiting 60 seconds to avoid hitting API limit")
    time.sleep(60)

    # Extract FX currency pairs
    exchange_rate = {}
    for currency in config['extract']['exchange_rate']['currency']:
        logger.info(f"Extracting: {currency} exchange rate API data")
        exchange_rate[currency] = Extract.fx_rate(to_symbol=currency, api_key=api_key)

    # Extract Crypto data
    crypto_price = {}
    for symbol in config['extract']['crypto_price']['symbols']:
        logger.info(f"Extracting: {symbol} in USD crypto prices API data")
        crypto_price[symbol] = Extract.crypto_price(symbol=symbol, market="USD", api_key=api_key)

    logger.info("Extraction complete")

    #
    # LOAD
    #
    logger.info("Commencing database load")
    logging.info(f'Using server: {os.environ.get("target_db_server_name")}')

    # Get Postgres engine for target database
    target_engine = PostgresDB.create_pg_engine('target')

    # Load treasury yield data (staging overwrite)
    for maturity in config['extract']['treasury_yield']['options']:
        df = treasury_data[maturity]
        logger.info(f"Loading: {maturity} treasury yield data to staging table")
        key_columns = Load.get_key_columns(maturity)
        table_name = f'raw_treasury_yield_{maturity}'
        Load.overwrite_to_database(
            df=df,
            table_name=table_name,
            engine=target_engine,
            key_columns=key_columns
        )

    # Load FX data
    for currency in config['extract']['exchange_rate']['currency']:
        df = exchange_rate[currency]
        logger.info(f"Loading: {currency} exchange rate data to staging table")
        key_columns = Load.get_key_columns(currency)
        table_name = f'raw_exchange_rate_{currency}'.lower()
        Load.overwrite_to_database(
            df=df,
            table_name=table_name,
            engine=target_engine,
            key_columns=key_columns
        )

    # Load crypto data
    for symbol in config['extract']['crypto_price']['symbols']:
        df = crypto_price[symbol]
        logger.info(f"Loading: {symbol} crypto price data to staging table")
        key_columns = Load.get_key_columns(symbol)
        table_name = f'raw_crypto_price_{symbol}'.lower()
        Load.overwrite_to_database(
            df=df,
            table_name=table_name,
            engine=target_engine,
            key_columns=key_columns
        )


    logger.info("Database load complete")

    return True


if __name__ == "__main__":

    # run the pipeline
    if pipeline():
        print("success")
