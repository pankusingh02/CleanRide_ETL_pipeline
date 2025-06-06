#Validates that data is present and not broken

from logger import get_logger

logger=get_logger("Monitor")

def check_dataframe(df, min_rows=1):
    if df is None or df.empty or len(df)<min_rows:
        logger.warning(f'Data validation failed. Rows: {len(df)}')
        return False
    
    logger.info(f'Data validation Passes. Rows: {len(df)}')
    return True