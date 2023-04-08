"""
This module contains the class to scrape articles from the "Welt" newspaper (https://www.welt.de/).
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
from selenium.common.exceptions import NoSuchElementException

from ..utils.logger import log
from ..scraper import NewspaperManager


class DeWelt(NewspaperManager):
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
        URL = f'https://www.welt.de/schlagzeilen/nachrichten-vom-{day.strftime("%-d-%-m-%Y")}.html'

        soup = BeautifulSoup(requests.get(URL).content, "html.parser")
        articles = soup \
            .find("div", {"class": "c-tabs__panel-content"}) \
            .find_all("article", {"class": "c-teaser c-teaser--archive"})

        # Get articles urls
        urls = ['https://www.welt.de' + article.find('h4').find('a')['href'] for article in articles]

        # Get articles publication dates
        time_regex = re.compile(r'\d{2}\.\d{2}\.\d{4}\s\|\s\d{2}:\d{2}')
        pub_dates = [pd.to_datetime(f'{article.find(string=time_regex)}', format='%d.%m.%Y | %H:%M')
                     for article in articles]
        # Add timezone Europe/Berlin to pub_dates
        pub_dates = [pub_date.tz_localize('Europe/Berlin') for pub_date in pub_dates]

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
            soup = BeautifulSoup(html, "html.parser")
            premium_icon = soup.find("header", {"class": "c-content-container"}).\
                find('a', {"class": "o-dreifaltigkeit__premium-badge"})
        except AttributeError:
            log.warning(f'Error scraping {url}.')
            return None, False
        return html, not bool(premium_icon)

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
        self.selenium_driver.get('https://www.welt.de/')
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="SP Consent Message"]'))
        )
        self.selenium_driver.switch_to.frame(privacy_frame)
        WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'button[title="Alle akzeptieren"]')))
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[title="Alle akzeptieren"]').click()
        # Wait and reload page because of ads
        time.sleep(10)
        self.selenium_driver.get('https://www.welt.de/')

        # Login
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[data-component="LoginButton"]').click()
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[data-qa="PageHeader.Login.Button.Login"]').click()
        WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.NAME, 'username')))
        self.selenium_driver.find_element(By.NAME, 'username').send_keys(username)
        self.selenium_driver.find_element(By.NAME, 'password').send_keys(password)
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()

        # Check if login was successful
        try:
            self.selenium_driver.get('https://www.welt.de/meinewelt/')
            WebDriverWait(self.selenium_driver, 10).until(
                ec.presence_of_element_located((By.CSS_SELECTOR, 'div[data-component-name="home"]')))
            _elem = self.selenium_driver.find_element(By.CSS_SELECTOR, 'div[data-component-name="home"]')
            WebDriverWait(_elem, 10).until(ec.presence_of_element_located((By.CSS_SELECTOR, 'div[name="greeting"]')))
            self.selenium_driver.get('https://www.welt.de')
            log.info('Logged in to Welt Plus.')
            return True
        except NoSuchElementException:
            log.warning('Login to Welt Plus failed.')
            return False
