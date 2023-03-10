"""
This module contains the Scraper class which is used to scrape articles from a newspaper website. It is only used as a
base class for the actual scrapers.
"""

import re
import datetime as dt

import pandas as pd
from goose3 import Goose
from tqdm import tqdm

from .utils.logger import CustomLogger
from .utils.utils import flatten_dict
from .utils.utils import get_selenium_webdriver
from .utils.utils import retry_on_exception
from .settings import settings
from .database import Database

# Declare logger
log = CustomLogger('newspaper-scraper', log_file=settings.log_file)


class Scraper:
    """
    The Scraper class is used to scrape articles from a newspaper website. It is only used as a base class for the
    actual scrapers and can not be instantiated directly. Each newspaper scraper needs to inherit from this class and
    implement the _get_published_articles, _soup_get_html, and _selenium_login methods.
    
    It contains the following methods:
        - index_published_articles: Scrapes all articles published between date_from and date_to.
        - scrape_public_articles: Checks for all indexed if they are public and scrapes them if they are.
        - scrape_private_articles: Scrapes all private articles. Needs a valid login.
        - _parse_article: Parses the HTML of an article and returns a DataFrame with the parsed infos.
        - _get_published_articles: Not implemented. Needs to be implemented in the actual scraper.
        - _soup_get_html: Not implemented. Needs to be implemented in the actual scraper.
        - _selenium_login: Not implemented. Needs to be implemented in the actual scraper.
    """

    def __init__(self, db_file):
        self._db = Database(db_file=db_file)

        self.newspaper_id = re.sub(r'(?<!^)(?=[A-Z])', '_', self.__class__.__name__).lower()
        self.selenium_driver = None

    def __enter__(self):
        self._db.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db.close()

    @staticmethod
    def _parse_article(html, url):
        """
        Parses the HTML of an article with goose3 and returns a DataFrame with the parsed infos.

        Args:
            html (str or bytes): The HTML of the article.
            url (str): The URL of the article.

        Returns:
            pd.DataFrame: A DataFrame with the parsed infos.
        """
        g = Goose()
        article = g.extract(raw_html=html)

        # Get all parsed infos
        parsed_infos = article.infos
        parsed_infos = flatten_dict(parsed_infos)
        # Change all keys to CamelCase
        parsed_infos = {''.join(word.title() for word in k.split('_')): v for k, v in parsed_infos.items()}

        # Change lists to pd.Series to be able to store them in the database
        for key, value in parsed_infos.items():
            if isinstance(value, list):
                parsed_infos[key] = pd.Series(value, dtype='object')

        parsed_infos = pd.DataFrame(parsed_infos, index=[url])
        parsed_infos.index.name = 'URL'

        return parsed_infos

    @retry_on_exception
    def index_published_articles(self, date_from, date_to, skip_existing=True):
        """
        Indexes all articles published between date_from and date_to for a given newspaper. Indexing means that the
        articles are added to the database and their URLs are stored. The actual scraping of the articles is done
        separately with the scrape_public_articles and scrape_private_articles methods.

        Args:
            date_from (dt.datetime or str): The first day to scrape articles from.
            date_to (dt.datetime or str): The last day to scrape articles from.
            skip_existing (bool, optional): If True, days that are already indexed are skipped. Defaults to True.
        """

        if date_from:
            date_from = pd.to_datetime(date_from)
            date_to = pd.to_datetime(date_to)

            date_range = [day for day in pd.date_range(date_from, date_to) if
                          not any([day.date() == index_day.date() for index_day in self._db.articles[
                              self._db.articles.NewspaperID == self.newspaper_id].PubDateIndexPage])
                          or not skip_existing]

            if len(date_range) == 0:
                log.info(f'No new days to scrape. Pass skip_existing=False to scrape all days again.')
                return

            log.info(f'Start scraping articles for {len(date_range):,} days ({date_from.strftime("%d.%m.%y")} - '
                     f'{date_to.strftime("%d.%m.%y")}). {len(pd.date_range(date_from, date_to)) - len(date_range)} '
                     f'days already indexed.')
            plog = tqdm(total=0, position=0, bar_format='{desc}')
            pbar = tqdm(total=len(date_range), position=1)
            counter = 0
            for day in date_range:
                counter += 1

                urls, pub_dates = self._get_published_articles(day)

                assert all([isinstance(pub_date, dt.datetime) for pub_date in pub_dates]), \
                    f'Not all pub_dates are datetime objects.'
                assert all([pub_date.tzinfo is not None for pub_date in pub_dates]), \
                    f'Not all pub_dates contain timezone info.'

                # Remove query strings from urls
                urls = [url.split('?')[0] for url in urls]

                assert all([isinstance(pub_date, dt.datetime) for pub_date in pub_dates]), \
                    f'Not all pub_dates are datetime objects.'

                urls = pd.DataFrame({'NewspaperID': self.newspaper_id,
                                     'PubDateIndexPage': pub_dates,
                                     'DateIndexed': dt.datetime.now()},
                                    index=urls)
                # Mark if urls are new
                urls['new'] = ~urls.index.isin(self._db.articles.index)

                # Add new urls to articles table
                self._db.articles = pd.concat([self._db.articles, urls[urls['new']].drop('new', axis=1)])
                pbar.update(1)
                plog.set_description_str(f'{counter}/{len(date_range)}: Indexed {urls.new.sum()}/{len(urls)} articles '
                                         f'for {day.strftime("%d.%m.%Y")}.')

                self._db.save_articles()

    @retry_on_exception
    def scrape_public_articles(self):
        """
        Checks for all indexed articles if they are publicly available. If they are, the article is scraped and
        the parsed infos are added to the database. If they are not, the article is marked as private. Uses 
        beautifulsoup4 to scrape the articles.
        """
        to_scrape = self._db.articles[(self._db.articles.NewspaperID == self.newspaper_id) &
                                      (self._db.articles.Public.isnull())]

        if to_scrape.empty:
            log.info(f'No articles to scrape.')
            return

        log.info(f'Start scraping {len(to_scrape):,} articles.')
        counter = 0
        plog = tqdm(total=0, position=0, bar_format='{desc}')
        pbar = tqdm(total=len(to_scrape), position=1)
        stats = []
        for url, row in to_scrape.iterrows():
            counter += 1
            raw_html, public = self._soup_get_html(url)

            if public:
                results = self._parse_article(raw_html, url)

                # Create empty column in articles table for each column in results
                for col in results.columns:
                    if col not in self._db.articles.columns:
                        self._db.articles[col] = None
                        log.info(f'New column detected in results. Added column {col} to articles table.')

                self._db.articles.update(results)
                self._db.articles.loc[url, 'DateScrapedHTML'] = dt.datetime.now()
                stats.append(1)

                plog.set_description_str(f'{counter}/{len(to_scrape)}: Article scraped. '
                                         f'(Stats: {stats.count(1)}/{stats.count(0)} '
                                         f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')

            else:
                stats.append(0)
                plog.set_description_str(f'{counter}/{len(to_scrape)}: Article is not public. '
                                         f'(Stats: {stats.count(1)}/{stats.count(0)} '
                                         f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')
            pbar.update(1)

            # Add results to articles table and save
            self._db.articles.loc[url, 'Public'] = public
            self._db.save_articles()

    @retry_on_exception
    def scrape_private_articles(self, username: str, password: str):
        """
        Scrapes all private articles, which have not been scraped yet and are marked as private. Uses selenium to
        scrape the articles. Requires a username and password to login to the newspaper website.

        Args:
            username (str): Username to login to the newspaper website.
            password (str): Password to login to the newspaper website.
        """
        to_scrape = self._db.articles[(self._db.articles.NewspaperID == self.newspaper_id) &
                                      (self._db.articles.Public == 0) &
                                      (self._db.articles.DateScrapedHTML.isnull())]

        # Return if no articles to scrape
        if to_scrape.empty:
            log.info(f'No articles to scrape.')
            return

        # Initialize selenium webdriver
        if self.selenium_driver is None:
            self.selenium_driver = get_selenium_webdriver()
            log.info('Selenium webdriver initialized.')

        # Login
        login_successful = self._selenium_login(username=username, password=password)
        if not login_successful:
            log.warning(f'Login failed. Skipping scraping.')
            return

        # Scrape articles
        log.info(f'Start scraping {len(to_scrape)} articles.')
        counter = 0
        plog = tqdm(total=0, position=0, bar_format='{desc}')
        pbar = tqdm(total=len(to_scrape), position=1)
        for url, row in to_scrape.iterrows():
            counter += 1

            # Scrape article
            self.selenium_driver.get(url)
            raw_html = self.selenium_driver.page_source
            results = self._parse_article(raw_html, url)

            # Add results to articles table and save
            self._db.articles.update(results)
            self._db.articles.loc[url, 'DateScrapedHTML'] = dt.datetime.now()
            self._db.save_articles()

            # Update progress bar
            pbar.update(1)
            plog.set_description_str(f'{counter}/{len(to_scrape)}: Article scraped.')

        self.selenium_driver.quit()

    def _get_published_articles(self, day):
        """
        Exists only as a placeholder. Needs to be implemented by the child class for each newspaper.
        """
        raise NotImplemented

    def _soup_get_html(self, url):
        """
        Exists only as a placeholder. Needs to be implemented by the child class for each newspaper.
        """
        raise NotImplemented

    def _selenium_login(self, username: str, password: str):
        """
        Exists only as a placeholder. Needs to be implemented by the child class for each newspaper.
        """
        raise NotImplemented
