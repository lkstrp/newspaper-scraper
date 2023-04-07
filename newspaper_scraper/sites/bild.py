"""
This module contains the class to scrape articles from the "Bild" newspaper (https://www.bild.de/).
The class inherits from the NewspaperManager class and needs an implementation of the abstract methods.
With a similar implementation, it is possible to scrape articles from other news websites.
"""
import re
import datetime as dt
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import ElementNotInteractableException

from ..utils.logger import log
from ..scraper import NewspaperManager


class DeBild(NewspaperManager):
    """
    This class inherits from the NewspaperManager class and implements the newspaper specific methods.
    These methods are:
        - _get_published_articles: Index articles published on a given day and return the urls and publication dates.
        - _soup_get_html: Determine if an article is premium content and scrape the html if it is not. Uses
            beautifulsoup.
        - _selenium_login: Login to the newspaper website to allow scraping of premium content after the login. Uses
            selenium.
    """

    def __init__(self, db_file: str = 'articles.db'):
        super().__init__(db_file)

    def _get_published_articles(self, day: dt.date):
        """
        Index articles published on a given day and return the urls and publication dates.

        Args:
            day (dt.date): Date of the articles to index.

        Returns:
            urls ([str]): List of urls of the articles published on the given day.
            pub_dates ([dt.datetime]): List of publication dates of the articles published on the given day.
              Needs timezone information.
        """
        URL = f'https://www.bild.de/themen/uebersicht/archiv/archiv-82532020.bild.html?archiveDate=' \
              f'{day.strftime("%Y-%m-%d")}'
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

    def _soup_get_html(self, url: str):
        """
        For a single article, determine if it is premium content and scrape the html if it is not.

        Args:
            url (str): Url of the article to scrape.

        Returns:
            html (str): Html of the article. If the article is premium content, None is returned.
            is_premium (bool): True if the article is premium content, False otherwise.
        """
        try:
            html = requests.get(url).content
            premium = re.search(r'https://www.bild.de/bild-plus/', url)

        except AttributeError:
            log.warning(f'Error scraping {url}.')
            return None, False

        return html, not bool(premium)

    def _selenium_login(self, username: str, password: str):
        """
        Using selenium, login to the newspaper website to allow scraping of premium content after the login. Does three
        things:
            1. Go to main page and accept cookies.
            2. Login.
            3. Check if login was successful.

        Args:
            username (str): Username to login to the newspaper website.
            password (str): Password to login to the newspaper website.

        Returns:
            bool: True if login was successful, False otherwise.
        """

        # Go to main page and accept cookies
        self.selenium_driver.get('https://www.bild.de/')
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="SP Consent Message"]'))
        )
        self.selenium_driver.switch_to.frame(privacy_frame)
        WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'button[title="Alle akzeptieren"]')))
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[title="Alle akzeptieren"]').click()
        # Wait and reload page because of ads
        time.sleep(10)
        self.selenium_driver.get('https://www.bild.de/')

        # Login
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[rel="nofollow"]').click()
        WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.NAME, 'username')))
        self.selenium_driver.find_element(By.NAME, 'username').send_keys(username)
        self.selenium_driver.find_element(By.NAME, 'password').send_keys(password)
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()

        # Check if login was successful
        try:
            WebDriverWait(self.selenium_driver, 10).until(
                ec.presence_of_element_located((By.CSS_SELECTOR, 'button[rel="nofollow"]')))
            log.info('Logged in to Bild Plus.')
            return True
        except ElementNotInteractableException:
            log.error('Login to Bild Plus failed.')
            return False
