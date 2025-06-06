# Filter out invalid rows (e.g., 0 distance/fare)
# Convert timestamps into datetime objects
# Add new columns like trip_duration_minutes

import pandas as pd
from utils.logger import get_logger
from utils.monitor import check_dataframe

logger = get_logger("Transformer")

def transform_sales_data(df: pd.DataFrame)-> pd.DataFrame:
    logger.info(f'Starting the Transformer')
    if not check_dataframe(df):
        raise ValueError ("Input data validation failed")
    
    df=df.copy()
    df=df[df['trip_distance']>0]
    df=df[df['fare_amount']>0]
    #Converts string dates like "2024-05-01 14:30:00" into datetime objects — so we can do calculations (e.g., durations).
    df['tpep_pickup_datetime'] =pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']= pd.to_datetime(df['tpep_dropoff_datetime'])

    #Adding new columns
    df['trip_duration_minutes']=(df['tpep_dropoff_datetime']-df['tpep_pickup_datetime']).dt.total_seconds()/60
    logger.info("Transformation complete. Final record count: {}".format(len(df)))
    return df
