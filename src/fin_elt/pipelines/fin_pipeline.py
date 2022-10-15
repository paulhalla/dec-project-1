from fin_elt.pipelines import extract_load
from database.postgres import PostgresDB
from fin_elt.elt.transform import Transform
from fin_elt.utility.metadata_logging import MetadataLogging
import logging
from io import StringIO
from graphlib import TopologicalSorter
import os
import yaml
import datetime as dt


def run_pipeline():
    # set up logging
    run_log = StringIO()
    logging.basicConfig(stream=run_log, level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

    # set up metadata logger
    metadata_logger = MetadataLogging(db_target="target")

    # Load config data
    with open(f"fin_elt/config.yaml") as stream:
        config = yaml.safe_load(stream)

    metadata_log_table = config["meta"]["log_table"]
    metadata_log_run_id = metadata_logger.get_latest_run_id(db_table=metadata_log_table)
    metadata_logger.log(
        run_timestamp=dt.datetime.now(),
        run_status="started",
        run_id=metadata_log_run_id,
        run_config=config,
        db_table=metadata_log_table
    )

    try:
        # configure pipeline
        logging.info("Getting config variables")
        path_transform_model = config["transform"]["model_path"]
        chunksize = config["load"]["chunksize"]
        # set up database
        target_engine = PostgresDB.create_pg_engine(db_target="target")

        # build dag
        dag = TopologicalSorter()
        nodes_extract_load = []

        logging.info("Run extract and load pipeline")
        # Extract and Load data to staging tables
        extract_load.pipeline()

        logging.info("Creating transform nodes")
        # transform nodes
        node_staging_treasury_yields = Transform(
            "stg_treasury_yields",
            engine=target_engine,
            models_path=path_transform_model
        )

        node_staging_exchange_rates = Transform(
            "stg_exchange_rates",
            engine=target_engine,
            models_path=path_transform_model
        )

        node_serving_exchange_rates = Transform(
            "srv_fx_rate_avgs",
            engine=target_engine,
            models_path=path_transform_model
        )

        node_serving_treasury_moving_avgs = Transform(
            "srv_try_moving_avgs",
            engine=target_engine,
            models_path=path_transform_model
        )
        dag.add(node_staging_treasury_yields)
        dag.add(node_staging_exchange_rates)
        dag.add(node_serving_treasury_moving_avgs, node_staging_treasury_yields)
        dag.add(node_serving_exchange_rates, node_staging_exchange_rates)

        logging.info("Executing DAG")
        # run dag
        dag_rendered = tuple(dag.static_order())
        for node in dag_rendered:
            node.run()

        logging.info("Pipeline run successful")
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="completed",
            run_id=metadata_log_run_id,
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )

    except Exception as e:
        logging.exception(e)
        metadata_logger.log(
            run_timestamp=dt.datetime.now(),
            run_status="error",
            run_id=metadata_log_run_id,
            run_config=config,
            run_log=run_log.getvalue(),
            db_table=metadata_log_table
        )

    print(run_log.getvalue())


if __name__ == "__main__":
    run_pipeline()
