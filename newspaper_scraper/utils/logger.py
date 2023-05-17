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

def change_log_level(logger: logging.Logger, new_log_level):
    """
    Changes the log level of the given logger. If the logger does not have a level set, the new level will be applied.
    The new_log_level can be provided as an integer or a string representing the log level name.

    Args:
        logger (logging.Logger): Logger to change the log level of.
        new_log_level: New log level (int or str).
    """
    level_name_to_level = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'NOTSET': logging.NOTSET
    }

    if isinstance(new_log_level, str):
        new_log_level = new_log_level.upper()
        if new_log_level not in level_name_to_level:
            raise ValueError(f"Invalid log level name '{new_log_level}'.")
        new_log_level = level_name_to_level[new_log_level]

    if isinstance(new_log_level, int) and (0 <= new_log_level <= 50):
        logger.setLevel(new_log_level)
    else:
        raise ValueError("Invalid log level. Log level must be an integer between 0 and 50 or a valid log level name.")
