"""
This module contains the package-wide settings.
"""
class _Settings:
    """
    This class contains the package-wide settings. It should not be instantiated and only used via the settings
    variable (see below).
    """

    retry_on_exception = True
    log_file = None
    save_interval = 60

    _description = {
        'retry_on_exception': 'If True, retry on exception.',
        'log_file': 'Path to log file. If None, no log file is created.',
    }

    def describe_settings(self):
        """Prints (not logs) all available settings and their description."""
        print('Available settings:')
        for key, value in self._description.items():
            print(f' \t- {key}: {value}')

    def get_current_settings(self):
        """Returns a dictionary with all current settings."""
        return {key: value for key, value in self.__dict__.items() if not key.startswith('_')}


settings = _Settings()
