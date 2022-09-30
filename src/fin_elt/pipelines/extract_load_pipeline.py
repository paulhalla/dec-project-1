from database.postgres import PostgresDB
from trading_price.etl.extract import Extract
from trading_price.etl.transform import Transform
from trading_price.etl.load import Load
import os
import logging


def pipeline() -> bool:
    logging.basicConfig(
        format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")  # format: https://docs.python.org/3/library/logging.html#logging.LogRecord
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)

    api_key_id = os.environ.get("api_key_id")
    api_secret_key = os.environ.get("api_secret_key")

    logger.info("Commencing extraction")
    # extract data
    df = Extract.extract(
        stock_ticker="tsla",
        api_key_id=api_key_id,
        api_secret_key=api_secret_key,
        start_date="2020-01-01",
        end_date="2020-01-05"
    )
    df_exchange_codes = Extract.extract_exchange_codes("trading_price/data/exchange_codes.csv")
    logger.info("Extraction complete")

    logger.info("Commencing transformation")
    # transform data
    df_transform = Transform.transform(
        df=df,
        df_exchange_codes=df_exchange_codes
    )
    logger.info("Transformation complete")

    # load file (upsert)
    logger.info("Commencing file load")
    Load.load(
        df=df_transform,
        load_target="file",
        target_file_directory="trading_price/data",
        target_file_name="tesla.parquet",
    )
    logger.info("File load complete")

    engine = PostgresDB.create_pg_engine()

    # load database (upsert)
    logger.info("Commencing database load")
    Load.load(
        df=df_transform,
        load_target="database",
        target_database_engine=engine,
        target_table_name="tesla_stock"
    )
    logger.info("Database load complete")
    return True


if __name__ == "__main__":

    # run the pipeline
    if pipeline():
        print("success")