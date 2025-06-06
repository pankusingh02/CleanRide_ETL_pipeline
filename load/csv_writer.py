# Save the processed pandas DataFrame to a CSV file.
# Customize the CSV output (like setting separator, including index or not).
# Handle exceptions during file writing.
# Log key events.

import pandas as pd
from utils.logger import get_logger

logger=get_logger("CSVWriter")

def save_to_csv(df:pd.DataFrame, file_path:str, seperator:str='', include_index: bool=False):
    try:
        logger.info(f'Saving dataframe to csv at: {file_path}')
        df.to_csv(file_path, sep=seperator,index= include_index)
        logger.info("file saved successfully")
    except Exception as e:
        logger.error(f'Failed to save the csv: {e}')
        raise