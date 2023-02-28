"""
TODO Docstring
"""

import datetime as dt
import logging
import os

logging.addLevelName(logging.DEBUG, 'D')
logging.addLevelName(logging.INFO, 'I')

# noinspection PyUnresolvedReferences,PyAttributeOutsideInit
class _LoggerTimeTaker(logging.Filter):
    """
    TODO DOCSTRING
    """

    def filter(self, record):
        """
        TODO DOCSTRING
        """
        try:
            last = self.last
        except AttributeError:
            last = record.relativeCreated

        delta = dt.datetime.fromtimestamp(record.relativeCreated / 1000.0) - dt.datetime.fromtimestamp(
            last / 1000.0)

        record.time_relative = '{0:.2f}'.format(delta.seconds + delta.microseconds / 1000000.0)

        self.last = record.relativeCreated
        return True


class _LoggerSingleLevelFilter(logging.Filter):
    """
    TODO DOCSTRING
    """

    def __init__(self, level):
        super().__init__()
        self.__level = level

    def filter(self, record):
        """
        TODO DOCSTRING
        """
        return record.levelno == self.__level


def _logger_emit_handler(self, record):
    terminal_width = os.get_terminal_size().columns
    msg_len = len(self.format(record)[:terminal_width])
    self.stream.write(self.format(record)[:msg_len]
                      + ' ' * (terminal_width - msg_len)
                      + self.terminator)


def setup_custom_logger(name, same_line_debug=False):
    """
    TODO
    """
    fmt_stream = logging.Formatter(
        fmt="[%(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
        datefmt="%y%m%d %H:%M:%S")
    fmt_file = logging.Formatter(
        fmt="[%(asctime)s %(time_relative)5s] %(levelname)s:%(lineno)d:%(funcName)s - %(message)s",
        datefmt="%y%m%d %H:%M:%S")

    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)

    h_stream = logging.StreamHandler()
    h_stream.setLevel(logging.DEBUG)
    h_stream.setFormatter(fmt_stream)
    log.addHandler(h_stream)

    if same_line_debug:
        h_stream_debug = logging.StreamHandler()
        h_stream_debug.terminator = '\r'
        h_stream_debug.emit = lambda record: _logger_emit_handler(h_stream_debug, record)
        h_stream_debug.addFilter(_LoggerSingleLevelFilter(logging.DEBUG))
        h_stream_debug.setFormatter(fmt_stream)
        log.addHandler(h_stream_debug)

        h_stream.setLevel(logging.INFO)

    # h_info = logging.FileHandler(f'logs/{name}_info.log')
    # h_info.setLevel(logging.INFO)
    # h_info.setFormatter(fmt_file)
    # log.addHandler(h_info)
    #
    # h_warning = logging.FileHandler(f'logs/{name}_errors.log')
    # h_warning.setLevel(logging.WARNING)
    # h_warning.setFormatter(fmt_file)
    # log.addHandler(h_warning)

    # todo Handle automatically cleaning of log files (just keep last x lines or something)
    [handler.addFilter(_LoggerTimeTaker()) for handler in log.handlers]

    return log
