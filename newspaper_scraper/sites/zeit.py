"""
This module contains the class to scrape articles from the "Zeit" newspaper (https://www.zeit.de/).
It does not scrape all articles published online, but only the ones that are published in the weekly edition.
The class inherits from the NewspaperManager class and needs an implementation of the abstract methods.
With a similar implementation, it is possible to scrape articles from other news websites.
"""

import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException

from ..utils.logger import log
from ..scraper import NewspaperManager


class DeZeit(NewspaperManager):
    """
    This class inherits from the NewspaperManager class and implements the newspaper specific methods.
    These methods are:
        - _get_articles_by_date: Index articles published in a given edition and return the urls and publication
        - _soup_get_html: Determine if an article is premium content and scrape the html if it is not. Uses
            beautifulsoup.
        - _selenium_login: Login to the newspaper website to allow scraping of premium content after the login. Uses
            selenium.
        - _selenium_get_html: Scrape the html of an article using selenium. Uses selenium. A specific implementation is
            needed here because the website uses pagination on some articles.
    """

    def __init__(self, db_file: str = 'articles.db'):
        super().__init__(db_file)

    def _get_articles_by_edition(self, year: int, edition: int):
        """
        Index articles published in a given edition and return the urls and publication dates.

        Args:
            year (int): Year of the edition to index.
            edition (int): Edition number of the edition to index. Can be similar to a week number, but is not
                necessarily the same.

        Returns:
        [str]: List of urls of the articles published on the given day.
        """
        # Get week number of day
        url = f'https://www.zeit.de/{year}/{edition:02}/index'

        html = self._request(url)
        if html is None:
            return []
        soup = BeautifulSoup(html, "html.parser")

        # Get list of article elements
        articles = soup.find_all("article")
        # Get articles urls
        urls = [article.find('a')['href'] for article in articles]
        urls = [url for url in urls if url.startswith('https://www.zeit.de/')]

        # Remove duplicates
        old_len = len(urls)
        urls = list(set(urls))
        log.warning(f"Removed {old_len - len(urls)} duplicate urls for {year}/{edition:02}.")

        return urls

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

        # Run again with /komplettansicht if a full page exists
        komplettansicht_link = soup.find("a", href=f"{url}/komplettansicht")
        if komplettansicht_link:
            return self._soup_get_html(f"{url}/komplettansicht")

        try:
            premium_icon = soup.find('aside', {'id': 'paywall'})
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
        self.selenium_driver.get('https://meine.zeit.de/anmelden')
        self.selenium_driver.find_element(By.XPATH, '//input[@type="email"]').send_keys(username)
        self.selenium_driver.find_element(By.XPATH, '//input[@type="password"]').send_keys(password)
        self.selenium_driver.find_element(By.XPATH, '//input[@type="submit"]').click()

        # Accept cookies
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="SP Consent Message"]')))
        self.selenium_driver.switch_to.frame(privacy_frame)
        cookie_accept_button = WebDriverWait(self.selenium_driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'AKZEPTIEREN UND WEITER')]")))
        cookie_accept_button.click()

        # Check if login was successful
        try:
            WebDriverWait(self.selenium_driver, 10).until(
                ec.presence_of_element_located((By.XPATH, '//span[@class="dashboard__title"]')))
            log.info('Logged in to Zeit Plus.')
            return True
        except TimeoutException:
            log.error('Login to Zeit Plus failed.')
            return False

    def _selenium_get_html(self, url):
        """
        Optional method to scrape the html of an article using selenium. A specific implementation is needed here
        because the website uses pagination on some articles.
        """
        self.selenium_driver.get(url)
        try:
            self.selenium_driver.find_element(By.XPATH, f"//a[@href='{url}/komplettansicht']")
            self.selenium_driver.get(url + '/komplettansicht')
        except NoSuchElementException:
            pass

        return self.selenium_driver.page_source
