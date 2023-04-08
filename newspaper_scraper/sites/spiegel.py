"""
This module contains the class to scrape articles from the "Spiegel" newspaper (https://www.spiegel.de/).
The class inherits from the NewspaperManager class and needs an implementation of the abstract methods.
With a similar implementation, it is possible to scrape articles from other news websites.
"""

import re
import locale
import datetime as dt

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import TimeoutException

from ..utils.logger import log
from ..scraper import NewspaperManager


class DeSpiegel(NewspaperManager):
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

        # Set locale to German
        locale.setlocale(locale.LC_TIME, "de_DE")

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
        URL = f'https://www.spiegel.de/nachrichtenarchiv/artikel-{day.strftime("%d.%m.%Y")}.html'
        soup = BeautifulSoup(requests.get(URL).content, "html.parser")
        articles = soup.find("section", {"data-area": "article-teaser-list"}) \
            .find_all("div", {"data-block-el": "articleTeaser"})

        # Remove advertisement articles
        articles = [article for article in articles if not article.find("h3")]

        # Get articles urls
        urls = [article.find('a')['href'] for article in articles]

        # Get articles publication dates
        time_regex = re.compile(r'\d{1,2}\.\s\w+,\s\d{1,2}\.\d{2}\sUhr')
        pub_dates = []
        for article in articles:
            try:
                pub_dates.append(pd.to_datetime(f'{article.find(string=time_regex)}; {day.year}',
                                                format='%d. %B, %H.%M Uhr; %Y').tz_localize('Europe/Berlin'))

            except ValueError:
                log.warning(f'Could not parse publication date for article {article.find("a")["href"]}: '
                            f'{article.find(string=time_regex)}.')
                pub_dates.append(dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc))

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
            premium_icon = soup.find("header", {"data-area": "intro"}).find('svg', {"id": "spon-spplus-flag-l"})
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
        self.selenium_driver.get('https://www.spiegel.de/')
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="Privacy Center"]')))
        self.selenium_driver.switch_to.frame(privacy_frame)
        self.selenium_driver.find_element(By.XPATH, "//button[contains(text(), 'Akzeptieren und weiter')]").click()

        # Login
        self.selenium_driver.get('https://gruppenkonto.spiegel.de/anmelden.html')
        self.selenium_driver.find_element(By.NAME, 'loginform:username').send_keys(username)
        self.selenium_driver.find_element(By.NAME, 'loginform:submit').click()
        self.selenium_driver.find_element(By.NAME, 'loginform:password').send_keys(password)
        self.selenium_driver.find_element(By.NAME, 'loginform:submit').click()

        # Go to main page
        self.selenium_driver.get('https://www.spiegel.de/')

        # Accept cookies again, if needed
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="Privacy Center"]')))
        self.selenium_driver.switch_to.frame(privacy_frame)
        self.selenium_driver.find_element(By.XPATH, "//button[contains(text(), 'Akzeptieren und weiter')]").click()

        # Click on Anmelden button because sometimes the login is not saved on main page
        try:
            self.selenium_driver.find_element(By.XPATH, '//a[@data-sara-link="gruppenkonto"]').click()
        except ElementNotInteractableException:
            pass

        # Check if loggin was successful
        self.selenium_driver.get('https://www.spiegel.de/')
        try:
            WebDriverWait(self.selenium_driver, 10).until(
                ec.presence_of_element_located((By.XPATH, '//a[@href="https://www.spiegel.de/fuermich/"]')))
            log.info('Logged in to Spiegel Plus.')
            return True
        except TimeoutException:
            log.error('Login to Spiegel Plus failed.')
            return False
