"""
This module contains the class to scrape articles from the "Welt" newspaper (https://www.welt.de/).
The class inherits from the NewspaperManager class and needs an implementation of the abstract methods.
With a similar implementation, it is possible to scrape articles from other news websites.
"""
import re
import datetime as dt

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
        - _get_articles_by_date: Index articles published on a given day and return the urls and publication dates.
        - _soup_get_html: Determine if an article is premium content and scrape the html if it is not. Uses
            beautifulsoup.
        - _selenium_login: Login to the newspaper website to allow scraping of premium content after the login. Uses
            selenium.
    """

    def __init__(self, db_file: str = 'articles.db'):
        super().__init__(db_file)

    def _get_articles_by_date(self, day: dt.date):
        """
        Index articles published on a given day and return the urls and publication dates.

        Args:
            day (dt.date): Date of the articles to index.

        Returns:
            [str]: List of urls of the articles published on the given day.
            [dt.datetime]: List of publication dates of the articles published on the given day. Needs timezone
                information.
        """
        url = f'https://www.welt.de/schlagzeilen/nachrichten-vom-{day.strftime("%-d-%-m-%Y")}.html'

        html = self._request(url)
        if html is None:
            return []
        soup = BeautifulSoup(html, "html.parser")

        # Get list of article elements
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
        pub_dates = [pub_date.tz_localize('UTC') for pub_date in pub_dates]

        return urls, pub_dates

    def _soup_get_html(self, url: str):
        """
        For a single article, determine if it is premium content and scrape the html if it is not.

        Args:
            url (str): Url of the article to scrape.

        Returns:
            str: Html of the article. If the article is premium content, None is returned.
            bool: True if the article is premium content, False otherwise.
        """
        html = self._request(url)
        if not html:
            return None, False
        soup = BeautifulSoup(html, "html.parser")
        try:
            premium_icon = soup.find("header", {"class": "c-content-container"}). \
                find('a', {"class": "o-dreifaltigkeit__premium-badge"})
            return html, not bool(premium_icon)
        except AttributeError:
            log.warning(f'Could not identify if article is premium: {url}.')
            return None, False

    def _selenium_login(self, username: str, password: str):
        """
        Using selenium, login to the newspaper website to allow scraping of premium content after the login.
        Args:
            username (str): Username to login to the newspaper website.
            password (str): Password to login to the newspaper website.

        Returns:
            bool: True if login was successful, False otherwise.
        """
        # Login
        self.selenium_driver.get('https://lo.la.welt.de/login')
        WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.NAME, 'username')))
        self.selenium_driver.find_element(By.NAME, 'username').send_keys(username)
        self.selenium_driver.find_element(By.NAME, 'password').send_keys(password)
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]').click()

        # Go to main page and accept cookies
        self.selenium_driver.get('https://www.welt.de/')
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="SP Consent Message"]'))
        )
        self.selenium_driver.switch_to.frame(privacy_frame)
        WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, 'button[title="Alle akzeptieren"]')))
        self.selenium_driver.find_element(By.CSS_SELECTOR, 'button[title="Alle akzeptieren"]').click()

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
