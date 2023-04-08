"""
This module contains some utility functions and decorators:
    - retry_on_exception: Decorator to retry a function if an exception is raised.
    - delay_interrupt: Decorator to delay keyboard interrupts.
    - get_selenium_webdriver: Returns a selenium webdriver object.
    - flatten_dict: Recursively flattens a nested dictionary.
"""
import sys
import os
import functools
import time
import signal

from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from ..utils.logger import log
from .. import settings


def retry_on_exception(func):
    """
    This decorator is used to retry a function if an exception is raised. This can be used during longer scraping
    processes to prevent the program from crashing due to a temporary network issue or similar.
    """

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except KeyboardInterrupt:
                sys.exit()
            except Exception as e:
                if settings.retry_on_exception:
                    log.warning(f'Exception while executing {func.__name__}: {e.__class__.__name__}.')
                    log.info('Retrying in 100 seconds...')
                    time.sleep(100)
                else:
                    raise e

    return _wrapper


def delay_interrupt(func):
    """
    This decorator is used to handle keyboard interrupts. It is applied to methods that perform long-running operations
    which should not be interrupted (like writings to the SQL database). It ignores keyboard interrupts while the
    function is running and then restores the interrupt handler when it is finished.

    Args:
        func (function): The function to be wrapped.
    """

    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        s = signal.signal(signal.SIGINT, signal.SIG_IGN)
        func(*args, **kwargs)
        signal.signal(signal.SIGINT, s)

    return _wrapper


# noinspection PyUnresolvedReferences
def get_selenium_webdriver():
    """
    Returns a selenium webdriver object.
    """
    # Windows
    if sys.platform == 'win32':
        from webdriver_manager.microsoft import EdgeChromiumDriverManager
        driver = webdriver.Edge(EdgeChromiumDriverManager().install())

    # MacOS
    elif sys.platform == 'darwin':
        driver = webdriver.Edge(service=Service(os.path.abspath(os.path.join(os.getcwd(), "..", "..", "Driver",
                                                                             "msedgedriver"))))

    # Linux
    elif sys.platform == 'linux':
        # noinspection PyPackageRequirements
        from webdriver.chrome.options import Options
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=chrome_options)

    else:
        raise Exception("Operating system couldn't be detected.")

    return driver


def flatten_dict(d, parent_key='', sep='_'):
    """
    Recursively flattens a nested dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
