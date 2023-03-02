"""
TODO DOCSTRING
"""
import os
import re
import datetime as dt
import locale

import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementNotInteractableException

from ..utils.logger import setup_custom_logger
from ..utils.utils import get_selenium_webdriver
from ..scraper import Scraper


# Declare logger
log = setup_custom_logger(os.path.basename(__file__)[:-3])


class DeSpiegel(Scraper):
    """
    TODO DOCSTRING
    """

    def __init__(self, username, password):
        super().__init__(username, password)

        # Set locale to German
        locale.setlocale(locale.LC_TIME, "de_DE")

    def get_published_articles(self, day):
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
        pub_dates = [dt.datetime.strptime(f'{article.find(string=time_regex)}; {day.year}', '%d. %B, %H.%M Uhr; %Y')
                     for article in articles]

        assert len(urls) == len(pub_dates), 'Number of urls and pub_dates does not match.'

        return urls, pub_dates

    def get_raw_html(self, url):
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

    def selenium_login(self):
        """
        TODO DOCSTRING
        """
        if self.selenium_driver is None:
            self.selenium_driver = get_selenium_webdriver()

        self.selenium_driver.get('https://gruppenkonto.spiegel.de/anmelden.html')
        self.selenium_driver.find_element(by=By.NAME, value='loginform:username').send_keys(self.usr)
        self.selenium_driver.find_element(by=By.NAME, value='loginform:submit').click()
        self.selenium_driver.find_element(by=By.NAME, value='loginform:password').send_keys(self.psw)
        self.selenium_driver.find_element(by=By.NAME, value='loginform:submit').click()
        self.selenium_driver.find_element(by=By.CSS_SELECTOR, value='a[class="tostart"]').click()

        # Accept cookies
        privacy_frame = WebDriverWait(self.selenium_driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//iframe[@title="Privacy Center"]'))
        )
        self.selenium_driver.switch_to.frame(privacy_frame)
        self.selenium_driver.find_element(By.XPATH, "//button[contains(text(), 'Akzeptieren und weiter')]").click()

        try:
            self.selenium_driver.find_element(by=By.XPATH, value='//a[@data-sara-link="gruppenkonto"]').click()
        except ElementNotInteractableException:
            pass

        log.info('Started Selenium Driver and logged in to Spiegel Plus.')

    def get_private_article_raw_html(self, url):
        """
        TODO DOCSTRING
        """
        self.selenium_driver.get(url)

        return self.selenium_driver.page_source
