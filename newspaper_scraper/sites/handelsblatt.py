"""
This module contains the class to scrape articles from the "Handelsblatt" newspaper (https://www.handelsblatt.com/).
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


class DeHandelsblatt(NewspaperManager):
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

    def _get_articles_by_date(self, day: dt.date):
        """
        Index articles published on a given day and return the urls and publication dates.

        Args:
            day (dt.date): Date of the articles to index.

        Returns:
            urls ([str]): List of urls of the articles published on the given day.
            pub_dates ([dt.datetime]): List of publication dates of the articles published on the given day.
              Needs timezone information.
        """
        URL = f'https://www.handelsblatt.com/archiv/{day.strftime("%Y/%-m/%-d")}'

        soup = BeautifulSoup(requests.get(URL).content, "html.parser")
        _articles = soup.find_all("a", {"class": "vhb-teaser-link"})
        urls = ['https://www.handelsblatt.com' + article['href'] for article in _articles]

        # Also add paginated articles
        pages_exist = soup.find("div", {"class": "vhb-teaser-pagination"})
        if pages_exist:
            page_urls = pages_exist.find("div", {"class": "vhb-tp-list"}).find_all('a')
            page_urls = ['https://www.handelsblatt.com' + page_url['href'] for page_url in page_urls]

            for page_url in page_urls:
                soup = BeautifulSoup(requests.get(page_url).content, "html.parser")
                _articles = soup.find_all("a", {"class": "vhb-teaser-link"})
                [urls.append('https://www.handelsblatt.com' + article['href']) for article in _articles]

        # Create list of publication dates, since the website does not provide them
        pub_dates = [dt.datetime.combine(day, dt.datetime.min.time(), tzinfo=dt.timezone.utc)] * len(urls)

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
        # Handelsblatt uses a login paywall via javascript. Therefore, selenium is needed to login. The following
        # return indicates that the article is premium content and they are all scraped self.scrape_premium_articles.
        return None, False

    def _selenium_login(self, username: str, password: str):
        """
        Using selenium, login to the newspaper website to allow scraping of premium content after the login. Does three
        things:
            1. Login
            2. Accept cookies
            3. Check if login was successful

        Args:
            username (str): Username to login to the newspaper website.
            password (str): Password to login to the newspaper website.

        Returns:
            bool: True if login was successful, False otherwise.
        """

        # Login
        self.selenium_driver.get('https://id.handelsblatt.com/login')
        self.selenium_driver.find_element(By.XPATH, '//input[@type="email"]').send_keys(username)
        self.selenium_driver.find_element(By.XPATH, '//input[@type="password"]').send_keys(password)
        self.selenium_driver.find_element(By.XPATH, '//button[@type="submit"]').click()
        self.selenium_driver.get('https://www.handelsblatt.com/ ')

        # Accept cookies
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="Iframe title"]')))
        self.selenium_driver.switch_to.frame(privacy_frame)
        cookie_accept_button = WebDriverWait(self.selenium_driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'ZUSTIMMEN')]")))
        cookie_accept_button.click()

        # Check if login was successful
        try:
            WebDriverWait(self.selenium_driver, 10).until(
                ec.presence_of_element_located((By.XPATH, f"//span[contains(text(), '{username}')]")))
            log.info('Logged in to Handelsblatt.')
            return True
        except TimeoutException:
            log.error('Login to Handelsblatt failed.')
            return False
