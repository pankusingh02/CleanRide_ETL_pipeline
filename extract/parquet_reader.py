import pandas as pd
import os
from utils.logger import get_logger #custom logging function 

#This creates a logger object with the name "ParquetReader" — used to print readable messages (like "starting read" or "error occurred") during the program run.
logger = get_logger("ParquetReader")

def read_parquet_file(file_path : str)-> pd.DataFrame:
    try:
        logger.info(f'Reading Parquet file from :{file_path}')
        df= pd.read_parquet(file_path)
        logger.info(f'Successfully read {len(df)} records.')
        return df
    except Exception as e:
        logger.error(f' Error reading the Parquet file: {e}')
        raise

#That raise makes sure the error is not silently ignored. It logs the problem and then lets it bubble up.