import os
import logging
from database.postgres import PostgresDB
from fin_elt.elt.extract import Extract


def pipeline() -> bool:
    logging.basicConfig(
        format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")  # format: https://docs.python.org/3/library/logging.html#logging.LogRecord
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)

    api_key = os.environ.get('AV_API_KEY')

    logger.info("Commencing extraction")
    # extract data
    for i in Extract.multiple_maturities(api_key=api_key):
        print(i[0], len(i[1]))
    logger.info("Extraction complete")

    engine = PostgresDB.create_pg_engine()

    # load database (staging overwrite)
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