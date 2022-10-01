import os
import logging
import yaml
from database.postgres import PostgresDB
from fin_elt.elt.extract import Extract
from fin_elt.elt.load import Load


def pipeline() -> bool:
    logging.basicConfig(format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)

    #
    # CONFIG
    #
    # Get Alpha Vantage API KEY from environment variable
    api_key = os.environ.get('AV_API_KEY')

    # Load config data
    with open(f"fin_elt/elt/config.yaml") as stream:
        config = yaml.safe_load(stream)

    #
    # EXTRACT
    #
    logger.info("Commencing extraction")

    # Extract treasury yields - for each maturity
    treasury_data = {}
    for i in config['extract']['treasury_yield']['options']:
        logger.info(f"Extracting: {i} treasury yield API data")
        treasury_data[i] = Extract.treasury_yields(interval='daily', maturity=i, api_key=api_key)

    # Extract FX currency pairs

    # Extract Crypto data

    logger.info("Extraction complete")

    #
    # LOAD
    #
    logger.info("Commencing database load")

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

    # Load crypto data

    logger.info("Database load complete")

    return True


if __name__ == "__main__":

    # run the pipeline
    if pipeline():
        print("success")
