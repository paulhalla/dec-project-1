from fin_elt.pipelines import extract_load
from database.postgres import PostgresDB
from fin_elt.elt.transform import Transform
import logging
import os


logging.basicConfig(level=logging.INFO, format="[%(levelname)s][%(asctime)s]: %(message)s")

# Extract and Load data to staging tables
extract_load.pipeline()

# Insert the latest day's treasury data to target table (combine yields into single table)

target_engine = PostgresDB.create_pg_engine()
path_transform_model = "fin_elt/models/transform"
node_staging_treasury_yields = Transform(
    "stg_treasury_yields",
    engine=target_engine,
    models_path=path_transform_model
).run()

