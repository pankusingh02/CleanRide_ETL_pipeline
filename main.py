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

logger= get_logger("MainETL")

def main():
    parquet_file= 'data.raw_sales.parquet'
    sql_db_path='data/sales.db'
    sql_query='SELECT * FROM sales_transaction'
    output_csv = "output/cleaned_sales.csv"

    try:
        logger.info("Starting the ETL pipeline")
        parquet_data= read_parquet_file(parquet_file)
        sql_data=read_sql_query(sql_db_path, sql_query)
        combined_data= parquet_data.append(sql_data, ignore_index=True) #ignore_index=True resets row numbers.
                # Step 2: Validate combined data before transform

        if not check_dataframe(combined_data):
            logger.error("No data to process in the extraction")


