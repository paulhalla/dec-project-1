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

    api_key = os.environ.get('AV_API_KEY')

    logger.info("Commencing extraction")
    # extract data

    logger.info("Extraction complete")

    engine = PostgresDB.create_pg_engine()

    # load database (upsert)
    logger.info("Commencing database load")

    logger.info("Database load complete")

    logger.info("Commencing transformation")
    # transform data

    logger.info("Transformation complete")

    return True


if __name__ == "__main__":

    # run the pipeline
    if pipeline():
        print("success")