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
        - _get_articles_by_edition: Index articles published in a given edition and return the urls and publication
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
            urls ([str]): List of urls of the articles published on the given day.
        """
        # Get week number of day
        URL = f'https://www.zeit.de/{year}/{edition:02}/index'

        soup = BeautifulSoup(requests.get(URL).content, "html.parser")
        articles = soup.find_all("article")

        # Get articles urls
        urls = [article.find('a')['href'] for article in articles]
        urls = [url for url in urls if url.startswith('https://www.zeit.de/')]
        # Remove duplicates
        urls = list(set(urls))

        return urls

    def _soup_get_html(self, url: str):
        """
        For a single article, determine if it is premium content and scrape the html if it is not.

        Args:
            url (str): Url of the article to scrape.

        Returns:
            html (str): Html of the article. If the article is premium content, None is returned.
            is_premium (bool): True if the article is premium content, False otherwise.
        """
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            komplettansicht_link = soup.find("a", href=f"{url}/komplettansicht")
            # Run again with /komplettansicht if it exists
            if komplettansicht_link:
                return self._soup_get_html(f"{url}/komplettansicht")
            else:
                try:
                    premium = soup.find('aside', {'id': 'paywall'})
                    return response.text, not bool(premium)
                except AttributeError:
                    log.warning(f'Error scraping {url}.')
                    return None, False
        else:
            log.warning(f"Error fetching the URL: {response.status_code}")
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

        # Check if loggin was successful
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
