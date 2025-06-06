# Extract data from Parquet & SQL
# Transform it using pandas
# Load the cleaned data to CSV
# Add logging, monitoring, and exception handling for a robust pipeline

import os
from extract.parquet_reader import read_parquet_file
from extract.sql_reader import read_sql_query
from transform.sales_transformer import transform_sales_data
from load.csv_writer import save_to_csv
from utils.logger import get_logger
from utils.monitor import check_dataframe
import pandas as pd

logger= get_logger("MainETL")

def main():
    parquet_file= 'data/raw_sales.parquet'
    sql_db_path='data/sales.db'
    sql_query='SELECT * FROM online_sales'
    output_csv = "output/cleaned_sales.csv"

    try:
        logger.info("Starting the ETL pipeline")
        parquet_data= read_parquet_file(parquet_file)
        sql_data=read_sql_query(sql_db_path, sql_query)
        combined_data = pd.concat([parquet_data, sql_data], ignore_index=True) #ignore_index=True resets row numbers.
                # Step 2: Validate combined data before transform

        if not check_dataframe(combined_data):
            logger.error("No data to process in the extraction")
            return
                # Step 3: Transform data
        transformed_data= transform_sales_data(combined_data)
                # Step 4: Validate transformed data before load
        if not check_dataframe(transformed_data):
            logger.error('No data to save after the transformation')
            return
                # Step 5: Load data to CSV
                # Create the output directory if missing.
                # Save the transformed DataFrame to CSV.
        os.makedirs(os.path.dirname(output_csv),exist_ok= True)
        save_to_csv(transformed_data,output_csv, seperator=',', include_index=False)
        logger.info("ETL pipeline completely succesfull")

    except Exception as e:
        logger.error(f'ETL pipeline failed {e}')


if __name__=='__main__':
    main()
