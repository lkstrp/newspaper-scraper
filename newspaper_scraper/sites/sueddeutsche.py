"""
This module contains the class to scrape articles from the "Süddeutsche Zeitung" newspaper
(https://www.sueddeutsche.de/).
The class inherits from the NewspaperManager class and needs an implementation of the abstract methods.
With a similar implementation, it is possible to scrape articles from other news websites.
"""
import datetime as dt
import time
import re

import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException

from ..utils.logger import log
from ..scraper import NewspaperManager


class DeSueddeutsche(NewspaperManager):
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
        Index articles published on a given day and return the urls and publication dates. The website list articles on
        a per day basis but per month and per category, so indexing works a bit different than for other newspapers.
        When the day of the date is 1, the function will index all articles for the given month. Otherwise it returns an
        empty list. Indexing included various requests since different categories have different urls. This takes
        longer than for other newspapers.

        Args:
            day (dt.date): Date of the articles to index. The function only return something if the day is the first of
                the month.

        Returns:
            [str]: List of urls of the articles published on the given day.
            [dt.datetime]: List of publication dates of the articles published on the given day. Needs timezone
                information.
        """
        if day.day != 1:
            return [], []

        # Get all categories
        bla = self._request('https://www.sueddeutsche.de/archiv/politik/2021')
        soup = BeautifulSoup(bla, "html.parser")
        categories = soup.find('select', {'id': 'dep'}).find_all('option')
        categories = [category['value'] for category in categories]
        categories.remove('none')

        # Handle categories
        urls = []
        for category in categories:
            # Handle pagination
            old_len = len(urls)
            page = 0
            while True:
                page += 1
                url = f'https://www.sueddeutsche.de/archiv/{category}/{day.strftime("%Y/%-m")}/page/{page}'
                html = self._request(url)
                if html is None:
                    return []
                soup = BeautifulSoup(html, "html.parser")
                # Get list of article elements
                articles = soup.find_all('div', {'class': 'entrylist__entry'})
                if not articles:
                    break

                # Filter out dpa articles
                articles = [article for article in articles if not (
                        article.find("span", class_="breadcrumb-list__item")
                        and article.find("span", class_="breadcrumb-list__item").text.strip() == "dpa")]

                # Add article urls to list
                [urls.append(article.find('a')['href']) for article in articles]
            log.info(f'Indexed {len(urls) - old_len} articles for category {category} in {day.strftime("%Y-%m")}.')

        # Remove query strings from urls (already here because this will create duplicates)
        urls = [url.split('?')[0] for url in urls]

        # Remove duplicates
        old_len = len(urls)
        urls = list(set(urls))
        if len(urls) < old_len:
            log.warning(f"Removed {old_len - len(urls)} duplicate urls for {day.strftime('%Y-%m-%d')}.")

        # Remove advertisements
        urls = [url for url in urls if not re.search(r'advertorial.sueddeutsche.de', url)]

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
        response = self._request(url, get_full_response=True)
        if not response:
            return None, False
        if response.history:
            redirected_url = response.url
        else:
            redirected_url = url

        premium = re.search(r'reduced=true', redirected_url)

        return response.text, not bool(premium)

    def _selenium_login(self, username: str, password: str):
        """
        Using selenium, login to the newspaper website to allow scraping of premium content after the login.
        Args:
            username (str): Username to login to the newspaper website.
            password (str): Password to login to the newspaper website.

        Returns:
            bool: True if login was successful, False otherwise.
        """
        # Accept cookies
        self.selenium_driver.get('https://www.sueddeutsche.de/')
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            ec.presence_of_element_located((By.XPATH, '//iframe[@title="SP Consent Message"]')))
        self.selenium_driver.switch_to.frame(privacy_frame)
        cookie_accept_button = WebDriverWait(self.selenium_driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Ich bin einverstanden')]")))
        cookie_accept_button.click()

        # Login
        self.selenium_driver.get('https://id.sueddeutsche.de/login')
        self.selenium_driver.find_element(By.XPATH, '//input[@name="login"]').send_keys(username)
        self.selenium_driver.find_element(By.XPATH, '//input[@name="password"]').send_keys(password)
        self.selenium_driver.find_element(By.XPATH, '//button[@type="submit"]').click()
        time.sleep(10)

        # Go to main page and click on login button (it is not saved)
        self.selenium_driver.get('https://www.sueddeutsche.de/')
        self.selenium_driver.find_element(By.XPATH, '//a[@class="custom-u47b19"]').click()

        # Check if login was successful
        try:
            WebDriverWait(self.selenium_driver, 10).until(
                ec.presence_of_element_located((By.XPATH, f"//a[contains(text(), 'Logout')]")))
            log.info('Logged in to Sueddeutsche Zeitung.')
            return True
        except TimeoutException:
            log.error('Login to Sueddeutsche Zeitung failed.')
            return False
