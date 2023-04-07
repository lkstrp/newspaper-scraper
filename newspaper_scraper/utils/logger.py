"""
Contains a custom logger class which uses some extra filters and handlers.
"""
import logging

logging.addLevelName(logging.DEBUG, 'D')
logging.addLevelName(logging.INFO, 'I')

log = logging.getLogger('newspaper-scraper')
log.setLevel(logging.DEBUG)

# Create formatters
fmt = logging.Formatter(fmt="[%(asctime)s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
                        datefmt="%y%m%d %H:%M:%S")

# Create stream handler
h_stream = logging.StreamHandler()
h_stream.setLevel(logging.DEBUG)
h_stream.setFormatter(fmt)
log.addHandler(h_stream)

# Create file handler
h_file = logging.FileHandler('logs.log', delay=True)
h_file.setLevel(logging.DEBUG)
h_file.setFormatter(fmt)
log.addHandler(h_file)

def change_log_file_path(logger: logging.Logger, new_log_file: str):
    """
    Changes the log file path of the given logger. If the logger does not have a file handler, a new one is created.

    Args:
        logger (logging.Logger): Logger to change the log file path of.
        new_log_file (str): New log file path.
    """
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            break
    if new_log_file:
        new_file_handler = logging.FileHandler(new_log_file)
        new_file_handler.setLevel(logging.DEBUG)
        new_file_handler.setFormatter(fmt)
        logger.addHandler(new_file_handler)
