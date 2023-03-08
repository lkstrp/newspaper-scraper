"""
TODO DOCSTRING
"""
import os
import re
import datetime as dt
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementNotInteractableException

from ..utils.logger import CustomLogger
from ..utils.utils import get_selenium_webdriver
from ..scraper import Scraper

# Declare logger
log = CustomLogger(os.path.basename(__file__)[:-3])


class DeBild(Scraper):
    """
    TODO DOCSTRING
    """

    def __init__(self, username=None, password=None):
        super().__init__(username, password)

    def _get_published_articles(self, day):
        """
        TODO DOCSTRING
        """
        URL = f'https://www.bild.de/themen/uebersicht/archiv/archiv-82532020.bild.html?archiveDate={day.strftime("%Y-%m-%d")}'
        soup = BeautifulSoup(requests.get(URL).content, "html.parser")
        articles = soup \
            .find("section", {"class": "stage-feed stage-feed--archive"}) \
            .find('ul', {'class': 'stage-feed__viewport'}) \
            .find_all('li')

        # Get articles urls
        urls = ['https://www.bild.de' + article.find('a')['href'] for article in articles]

        # Get articles publication dates
        pub_dates = [pd.to_datetime(article.find('time')['datetime']) for article in articles]

        assert len(urls) == len(pub_dates), 'Number of urls and pub_dates does not match.'

        return urls, pub_dates

    def _soup_get_html(self, url):
        """
        TODO DOCSTRING
        """
        try:
            html = requests.get(url).content
            premium = re.search(r'https://www.bild.de/bild-plus/', url)

        except AttributeError:
            log.warning(f'Error scraping {url}.')
            return None, False

        return html, not bool(premium)

    def _selenium_login(self):
        """
        TODO DOCSTRING
        """
        if self.usr is None or self.psw is None:
            raise ValueError('Username and password must be provided to login.')
        if self.selenium_driver is None:
            self.selenium_driver = get_selenium_webdriver()
            log.info('Selenium webdriver initialized.')

        # Go to main page and accept cookies
        self.selenium_driver.get('https://www.bild.de/')
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//iframe[@title="SP Consent Message"]'))
        )
        self.selenium_driver.switch_to.frame(privacy_frame)
        WebDriverWait(self.selenium_driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'button[title="Alle akzeptieren"]')))
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[title="Alle akzeptieren"]').click()
        # Wait and reload page because of ads
        time.sleep(10)
        self.selenium_driver.get('https://www.bild.de/')

        # Login
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[rel="nofollow"]').click()
        WebDriverWait(self.selenium_driver, 10).until(
            EC.presence_of_element_located((By.NAME, 'username')))
        self.selenium_driver.find_element(By.NAME, 'username').send_keys(self.usr)
        self.selenium_driver.find_element(By.NAME, 'password').send_keys(self.psw)
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()

        # Check if login was successful
        try:
            WebDriverWait(self.selenium_driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'button[rel="nofollow"]')))
            log.info('Logged in to Bild Plus.')
            return True
        except ElementNotInteractableException:
            log.error('Login to Bild Plus failed.')
            return False

