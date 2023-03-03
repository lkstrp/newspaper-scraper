"""
Contains a custom logger class which uses some extra filters and handlers.
"""

import datetime as dt
import logging
import os

logging.addLevelName(logging.DEBUG, 'D')
logging.addLevelName(logging.INFO, 'I')


# noinspection PyUnresolvedReferences,PyAttributeOutsideInit
class FilterTimeTaker(logging.Filter):
    """Filter that takes the time since the last log message. This can be used as 'time_relative' in the log format."""

    def filter(self, record):
        """Overwrites the filter method to take the time since the last log message."""
        try:
            last = self.last
        except AttributeError:
            last = record.relativeCreated

        delta = dt.datetime.fromtimestamp(record.relativeCreated / 1000.0) - dt.datetime.fromtimestamp(
            last / 1000.0)

        record.time_relative = '{0:.2f}'.format(delta.seconds + delta.microseconds / 1000000.0)

        self.last = record.relativeCreated
        return True


class FilterSingleLevel(logging.Filter):
    """Filter which only passes log messages of a single level.

    Args:
        level (_type_): Level of the log messages to pass.
    """

    def __init__(self, level):
        super().__init__()
        self.__level = level

    def filter(self, record):
        """Overwrites the filter method to only pass log messages of a single level."""
        return record.levelno == self.__level


class SameLineStreamHandler(logging.StreamHandler):
    """Stream handler which overwrites the emit method to print the log message on the same line.
    Can be used to print debug messages on the same line similar to a progress bar.

    Args:
        level (_type_): Level where the log message should be printed on the same line.
    """

    def __init__(self, level):
        super().__init__()
        self.terminator = '\r'
        self.emit = self.logger_emit_handler
        self.addFilter(FilterSingleLevel(level))

    def logger_emit_handler(self, record):
        """Overwrites the emit method to print the log message on the same line."""
        terminal_width = os.get_terminal_size().columns
        msg_len = len(self.format(record)[:terminal_width])
        self.stream.write(self.format(record)[:msg_len]
                          + ' ' * (terminal_width - msg_len)
                          + self.terminator)


class CustomLogger(logging.getLoggerClass()):
    """Custom logger class which adds a stream handler and a file handler.

    Args:
        name (str): Name of the logger.
        same_line_debug (bool, optional): If True, debug messages will be printed on the same line as the previous log
            message.
        log_file (str, optional): Path to the log file.
    """

    def __init__(self, name: str, same_line_debug: bool = False, log_file: str = None):

        super().__init__(name)
        self.setLevel(logging.DEBUG)

        # Create formatters
        fmt_stream = logging.Formatter(fmt="[%(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
                                       datefmt="%y%m%d %H:%M:%S")
        fmt_file = logging.Formatter(fmt="[%(asctime)s %(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %("
                                         "message)s",
                                     datefmt="%y%m%d %H:%M:%S")

        # Create stream handler
        self.h_stream = logging.StreamHandler()
        self.h_stream.setLevel(logging.DEBUG)
        self.h_stream.setFormatter(fmt_stream)
        self.addHandler(self.h_stream)

        if same_line_debug:
            self.h_stream_debug = SameLineStreamHandler(logging.DEBUG)
            self.h_stream_debug.setFormatter(fmt_stream)
            self.addHandler(self.h_stream_debug)

            self.h_stream.setLevel(logging.INFO)

        if log_file:
            self.h_file = logging.FileHandler(log_file)
            self.h_file.setLevel(logging.DEBUG)
            self.h_file.setFormatter(fmt_file)
            self.addHandler(self.h_file)

        # todo Handle automatically cleaning of log files (just keep last x lines or something)
        [handler.addFilter(FilterTimeTaker()) for handler in self.handlers]
