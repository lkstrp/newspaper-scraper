"""
TODO DOCSTRING
"""
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service


def get_selenium_webdriver():
    if sys.platform == 'win32':
        from webdriver_manager.microsoft import EdgeChromiumDriverManager
        driver = webdriver.Edge(EdgeChromiumDriverManager().install())

    # MacOS
    elif sys.platform == 'darwin':
        driver = webdriver.Edge(service=Service("/Users/lukas/lt_data/Code/driver/msedgedriver"))

    # Linux
    elif sys.platform == 'linux':
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