from database.postgres import PostgresDB
import datetime as dt 
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, JSON
from sqlalchemy import insert, select, func

class MetadataLogging():

    def __init__(self, db_target):
        self.engine = PostgresDB.create_pg_engine(db_target=db_target)
    
    def create_target_table_if_not_exists(self, db_table:str)->Table:
        meta = MetaData()
        target_table = Table(
            db_table, meta, 
            Column("run_timestamp", String, primary_key=True),
            Column("run_id", Integer, primary_key=True),
            Column("run_status", String, primary_key=True),
            Column("run_config", JSON),
            Column("run_log", String)
        )
        meta.create_all(self.engine) # creates table if it does not exist
        return target_table 
    
    def get_latest_run_id(self, db_table:str)->int:
        target_table = self.create_target_table_if_not_exists(db_table=db_table)
        statement = (
            select(func.max(target_table.c.run_id))
        )
        response = self.engine.execute(statement).first()[0]
        if response is None: 
            return 1 
        else: 
            return response + 1 

    def log(
        self,
        run_timestamp: dt.datetime,
        run_id: int,
        run_config: dict,
        db_table: str,
        run_status: str="started",
        run_log:str="",
    )->bool:
        target_table = self.create_target_table_if_not_exists(db_table=db_table)
        insert_statement = insert(target_table).values(
            run_timestamp=run_timestamp,
            run_id=run_id,
            run_status=run_status,
            run_config=run_config,
            run_log=run_log
        )
        self.engine.execute(insert_statement)

        return True 
