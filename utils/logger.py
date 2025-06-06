import logging #Python’s built-in logging library — helps write cleaner messages to console or files.

#Defines a function that creates and returns a Logger object with a given name (e.g., "SQLReader", "ParquetReader").
def get_logger(name:str)-> logging.Logger:
    logger=logging.getLogger(name) #Gets or creates a logger with the specified name.
    if not logger.handlers:
        logger.setLevel(logging.INFO) #Sets the logging level to INFO (so INFO and ERROR messages are shown).
        ch=logging.StreamHandler()
        ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(ch)
        return logger

# Creates a new stream handler (outputs to console).
# Formats it to show time, logger name, log level, and message.
# Adds this handler to our logger.

