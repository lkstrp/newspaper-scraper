"""
TODO DOCSTRING
"""
import os
import re
import locale

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementNotInteractableException
from selenium.common.exceptions import TimeoutException

from ..utils.logger import CustomLogger
from ..utils.utils import get_selenium_webdriver
from ..scraper import Scraper

# Declare logger
log = CustomLogger(os.path.basename(__file__)[:-3], log_file='logs.log')


class DeSpiegel(Scraper):
    """
    TODO DOCSTRING
    """

    def __init__(self, username=None, password=None):
        super().__init__(username, password)

        # Set locale to German
        locale.setlocale(locale.LC_TIME, "de_DE")

    def _get_published_articles(self, day):
        """
        TODO DOCSTRING
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
        pub_dates = [pd.to_datetime(f'{article.find(string=time_regex)}; {day.year}', format='%d. %B, %H.%M Uhr; %Y')
                     for article in articles]
        # Add timezone Europe/Berlin to pub_dates
        pub_dates = [pub_date.tz_localize('Europe/Berlin') for pub_date in pub_dates]

        assert len(urls) == len(pub_dates), 'Number of urls and pub_dates does not match.'

        return urls, pub_dates

    def _soup_get_html(self, url):
        """
        TODO DOCSTRING
        """
        try:
            html = requests.get(url).content
            soup = BeautifulSoup(html, "html.parser")
            premium_icon = soup.find("header", {"data-area": "intro"}).find('svg', {"id": "spon-spplus-flag-l"})
        except AttributeError:
            log.warning(f'Error scraping {url}.')
            return None, False
        return html, not bool(premium_icon)

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
        self.selenium_driver.get('https://www.spiegel.de/')
        privacy_frame = WebDriverWait(self.selenium_driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//iframe[@title="Privacy Center"]')))
        self.selenium_driver.switch_to.frame(privacy_frame)
        self.selenium_driver.find_element(By.XPATH, "//button[contains(text(), 'Akzeptieren und weiter')]").click()

        # Login
        self.selenium_driver.get('https://gruppenkonto.spiegel.de/anmelden.html')
        self.selenium_driver.find_element(By.NAME, 'loginform:username').send_keys(self.usr)
        self.selenium_driver.find_element(By.NAME, 'loginform:submit').click()
        self.selenium_driver.find_element(By.NAME, 'loginform:password').send_keys(self.psw)
        self.selenium_driver.find_element(By.NAME, 'loginform:submit').click()

        # Click on Anmelden button because sometimes the login is not saved on main page
        self.selenium_driver.get('https://www.spiegel.de/')
        try:
            self.selenium_driver.find_element(By.XPATH, '//a[@data-sara-link="gruppenkonto"]').click()
        except ElementNotInteractableException:
            pass

        # Check if loggin was successful
        self.selenium_driver.get('https://www.spiegel.de/')
        try:
            WebDriverWait(self.selenium_driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//a[@href="https://www.spiegel.de/fuermich/"]')))
            log.info('Logged in to Spiegel Plus.')
            return True
        except TimeoutException:
            log.error('Login to Spiegel Plus failed.')
            return False
