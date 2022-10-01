from sqlalchemy import Integer, String, Float, JSON , DateTime, Boolean, BigInteger, Numeric
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, JSON 
import jinja2 as j2 
import pandas as pd
import numpy as np
import logging 
from sqlalchemy.dialects import postgresql
class Load():

    def load_fx(
        df:pd.DataFrame,
        load_method:str="overwrite",
        target_database_engine=engine,
        target_table_name:str=None
        )->None:
        """
        Load dataframe to either a file or a database. 
        - df: pandas dataframe to load.  
        - load_method: choose either `overwrite` or `upsert`. defaults to `overwrite`.
        - target_database_engine: SQLAlchemy engine for the target database. 
        - target_table_name: name of the SQL table to create and/or upsert data to. 
        """
        if load_method.lower() == "overwrite": 
            df.to_sql(target_table_name, target_database_engine)
        elif load_method.lower() == "upsert":
            meta = MetaData()
            fx_table = Table(
                target_table_name, meta, 
                Column("date", String, primary_key=True),
                Column("open_value", Integer),
                Column("high_value", String),
                Column("low_value", Float),
                Column("close_value", Integer),
                # Column("from", String, primary_key=True),  # Won't use in case we do only USD in the FROM column
                Column("to", String, primary_key=True)
            )
            meta.create_all(target_database_engine) # creates table if it does not exist 
            insert_statement = postgresql.insert(fx_table).values(df.to_dict(orient='records'))
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=['date', 'to'],
                set_={c.key: c for c in insert_statement.excluded if c.key not in ['date', 'to']})
            target_database_engine.execute(upsert_statement)