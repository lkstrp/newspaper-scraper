"""
This module contains the package-wide settings.
"""
from .utils import logger

class _Settings:
    """
    This class contains the package-wide settings. It should not be instantiated and only used via the settings
    variable (see below).
    """

    _log_file = 'logs.log'
    _log_level = 'DEBUG'
    retry_on_exception = True
    save_interval = 60
    selenium_driver = None

    _description = {
        'log_file': 'Path to the log file. If None, no log file is created.',
        'log_level': 'Log level. Possible values: DEBUG, INFO, WARNING, ERROR, CRITICAL.',
        'retry_on_exception': 'If True, retry on exception.',
        'save_interval': 'Interval in seconds to save the database. Smaller values will slow down the process.',
        'selenium_driver': 'Selenium driver object. Pass a driver object to use it if the provided one does not work.'
    }

    # noinspection PyMissingOrEmptyDocstring
    @property
    def log_level(self):
        return self._log_level

    @log_level.setter
    def log_level(self, value):
        self._log_level = value
        logger.change_log_level(logger.log, value)

    # noinspection PyMissingOrEmptyDocstring
    @property
    def log_file(self):
        return self._log_file

    @log_file.setter
    def log_file(self, value):
        self._log_file = value
        logger.change_log_file_path(logger.log, value)

    def describe_settings(self):
        """Prints (not logs) all available settings and their description."""
        print('Available settings:')
        for key, value in self._description.items():
            print(f' \t- {key}: {value}')

    def get_current_settings(self):
        """Returns a dictionary with all current settings."""
        return {key: value for key, value in self.__dict__.items() if not key.startswith('_')}


settings = _Settings()
