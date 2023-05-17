"""
This module contains the class to scrape articles from the "Tagesspiegel" newspaper (https://www.tagesspiegel.de/).
The class inherits from the NewspaperManager class and needs an implementation of the abstract methods.
With a similar implementation, it is possible to scrape articles from other news websites.
"""
import datetime as dt

import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException

from ..utils.logger import log
from ..scraper import NewspaperManager


class DeTagesspiegel(NewspaperManager):
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
        url = f'https://www.tagesspiegel.de/archiv/{day.strftime("%Y/%m/%d")}/'

        # Handle pagination
        urls = []
        page = 0
        while True:
            page += 1
            html = self._request(url + (f'{page}' if page > 1 else ''))
            if not html:
                break
            soup = BeautifulSoup(html, "html.parser")
            # Get list of article elements
            articles = soup.find_all('article')
            # Add article urls to list
            [urls.append('https://www.tagesspiegel.de' + article.find('a')['href']) for article in articles]

        # Remove duplicates
        old_len = len(urls)
        urls = list(set(urls))
        if len(urls) < old_len:
            log.warning(f"Removed {old_len - len(urls)} duplicate urls for {day.strftime('%Y-%m-%d')}.")

        # Create list of publication dates, since the website does not provide them
        pub_dates = [dt.datetime.combine(day, dt.datetime.min.time(), tzinfo=dt.timezone.utc)] * len(urls)

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
        soup = BeautifulSoup(html, "html.parser")
        try:
            premium_icon = soup.find('div', {'class': 'Uk'}).find('svg')
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
        # Accept cookies on main page
        self.selenium_driver.get('https://www.tagesspiegel.de/')
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="Iframe title"]')))
        self.selenium_driver.switch_to.frame(privacy_frame)
        cookie_accept_button = WebDriverWait(self.selenium_driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Alle akzeptieren')]")))
        cookie_accept_button.click()

        # Login
        self.selenium_driver.find_element(By.XPATH,
                                          '//a[@title="Melden Sie sich mit Ihrem Tagesspiegel-Konto an"]').click()
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@id="modal-iframe"]')))
        self.selenium_driver.switch_to.frame(privacy_frame)
        WebDriverWait(self.selenium_driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, '//input[@name="email"]')))
        self.selenium_driver.find_element(By.XPATH, '//input[@name="email"]').send_keys(username)
        self.selenium_driver.find_element(By.XPATH, '//input[@name="password"]').send_keys(password)
        self.selenium_driver.find_element(By.XPATH, '//button[@type="submit"]').click()

        # Check if login was successful
        try:
            WebDriverWait(self.selenium_driver, 10).until(
                ec.presence_of_element_located((By.XPATH, "//span[contains(text(), 'Mein Konto')]")))
            log.info('Logged in to Tagesspiegel.')
            return True
        except TimeoutException:
            log.error('Login to Tagesspiegel failed.')
            return False
