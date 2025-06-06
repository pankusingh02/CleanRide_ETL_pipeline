import pandas as pd
import sqlite3
from utils.logger import get_logger

logger= get_logger("SQLReader")

def read_sql_query(db_path: str, query:str)-> pd.DataFrame:
    try:
        logger.info(f'Connecting to database: {db_path}')
        conn=sqlite3.connect(db_path)
        df= pd.read_sql_query(query, conn)
        conn.close
        logger.info(f'Successfully read {len(df)} rows from the sql')
        return df
    except Exception as e:
        logger.error(f'Sql query failed {e}')
        raise