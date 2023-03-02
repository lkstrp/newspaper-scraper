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
