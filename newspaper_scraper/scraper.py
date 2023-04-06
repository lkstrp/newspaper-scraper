"""
This module contains the NewspaperManager class which is used to scrape articles from a newspaper website. It is only
used as a base class for the actual scrapers.
"""

import re
import datetime as dt

import pandas as pd
from goose3 import Goose
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import spacy

from .utils.logger import CustomLogger
from .utils.utils import flatten_dict
from .utils.utils import get_selenium_webdriver
from .utils.utils import retry_on_exception
from .settings import settings
from .database import Database

# Declare logger
log = CustomLogger('newspaper-scraper', log_file=settings.log_file)


class NewspaperManager:
    """
    The NewspaperManager class is used to scrape articles from a newspaper website. It is only used as a base class for the
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
        self.spacy = None

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
        # Rename CleanedText to Text
        parsed_infos = parsed_infos.rename({'CleanedText': 'Text'}, axis=1)
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
                          not any([day.date() == index_day.date() for index_day in self._db.df_indexed[
                              self._db.df_indexed.NewspaperID == self.newspaper_id].PubDateIndexPage])
                          or not skip_existing]

            if len(date_range) == 0:
                log.info(f'No new days to scrape. Pass skip_existing=False to scrape all days again.')
                return

            log.info(f'Start scraping articles for {len(date_range):,} days ({date_from.strftime("%d.%m.%y")} - '
                     f'{date_to.strftime("%d.%m.%y")}). {len(pd.date_range(date_from, date_to)) - len(date_range)} '
                     f'days already indexed.')
            counter = 0
            with logging_redirect_tqdm(loggers=[log]):
                for day in tqdm(date_range):
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
                    urls['new'] = ~urls.index.isin(self._db.df_indexed.index)

                    # Add new urls to articles table
                    self._db.df_indexed = pd.concat([self._db.df_indexed, urls[urls['new']].drop('new', axis=1)])
                    log.info(f'{counter}/{len(date_range)}: Indexed {urls.new.sum()}/{len(urls)} articles '
                             f'for {day.strftime("%d.%m.%Y")} (n={len(self._db.df_indexed):,}).')

                    self._db.save_data('df_indexed', mode='replace')

    @retry_on_exception
    def scrape_public_articles(self):
        """
        Checks for all indexed articles if they are publicly available. If they are, the article is scraped and
        the parsed infos are added to the database. If they are not, the article is marked as private. Uses 
        beautifulsoup4 to scrape the articles.
        """
        to_scrape = self._db.df_indexed[(self._db.df_indexed.NewspaperID == self.newspaper_id) &
                                        (self._db.df_indexed.Public.isnull())]

        if to_scrape.empty:
            log.info(f'No articles to scrape.')
            return

        log.info(f'Start scraping {len(to_scrape):,} articles.')
        counter = 0
        stats = []
        with logging_redirect_tqdm(loggers=[log]):
            for url, row in tqdm(to_scrape.iterrows(), total=len(to_scrape)):
                counter += 1
                raw_html, public = self._soup_get_html(url)

                if public:
                    results = self._parse_article(raw_html, url)

                    # Notifies if new columns are detected in results
                    for col in results.columns:
                        if col not in self._db.df_scraped_cols:
                            self._db.df_scraped_cols.append(col)
                            log.info(f'New column detected: {col}')  # todo print can be removed

                    # Add results to articles table
                    self._db.df_scraped_new = pd.concat([self._db.df_scraped_new, results], axis=0)
                    self._db.df_scraped_new.loc[url, 'DateScrapedHTML'] = dt.datetime.now()
                    self._db.df_indexed.at[url, 'Scraped'] = True

                    stats.append(1)
                    # pbar.write('Wurst')
                    log.info(f'{counter}/{len(to_scrape)}: Article scraped. '
                             f'(Stats: {stats.count(1)}/{stats.count(0)} '
                             f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')

                else:
                    stats.append(0)
                    log.info(f'{counter}/{len(to_scrape)}: Article is not public. '
                             f'(Stats: {stats.count(1)}/{stats.count(0)} '
                             f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')

                # Add public information and save both tables
                self._db.df_indexed.at[url, 'Public'] = public
                self._db.save_data('df_indexed', mode='replace')
                self._db.save_data('df_scraped', mode='append')

    @retry_on_exception
    def scrape_private_articles(self, username: str, password: str):
        """
        Scrapes all private articles, which have not been scraped yet and are marked as private. Uses selenium to
        scrape the articles. Requires a username and password to login to the newspaper website.

        Args:
            username (str): Username to login to the newspaper website.
            password (str): Password to login to the newspaper website.
        """
        to_scrape = self._db.df_indexed[(self._db.df_indexed.NewspaperID == self.newspaper_id) &
                                        (self._db.df_indexed.Public == 0) &
                                        (self._db.df_indexed.Scraped != True)]

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
        with logging_redirect_tqdm(loggers=[log]):
            for url, row in tqdm(to_scrape.iterrows(), total=len(to_scrape)):
                counter += 1

                # Scrape article
                self.selenium_driver.get(url)
                raw_html = self.selenium_driver.page_source
                results = self._parse_article(raw_html, url)

                # Add results to articles table
                self._db.df_scraped_new = pd.concat([self._db.df_scraped_new, results], axis=0)
                self._db.df_scraped_new.loc[url, 'DateScrapedHTML'] = dt.datetime.now()
                self._db.df_indexed.at[url, 'Scraped'] = True

                # Save both tables
                self._db.save_data('df_indexed', mode='replace')
                self._db.save_data('df_scraped', mode='append')

        self.selenium_driver.quit()

    def nlp(self):
        """
        TODO
        """
        to_process = self._db.df_scraped[(self._db.df_indexed.NewspaperID == self.newspaper_id) &
                                            (self._db.df_indexed.Processed != True)]

        # Return if no articles to scrape
        if to_process.empty:
            log.info(f'No articles to process.')
            return

        # Initialize spacy:
        if self.spacy is None:
            self.spacy = spacy.load('de_core_news_sm') # todo: add language as parameter
            log.info('Spacy initialized.')

        # Process articles
        log.info(f'Start processing {len(to_process):,} articles.')
        counter = 0
        with logging_redirect_tqdm(loggers=[log]):
            for url, row in tqdm(to_process.iterrows(), total=len(to_process)):
                counter += 1

                # Process article
                results = self._process_article(row['Text'], url)

                # Add results to articles table
                self._db.df_processed_new = pd.concat([self._db.df_processed_new, results], axis=0)
                self._db.df_processed_new.loc[url, 'DateProcessed'] = dt.datetime.now()
                self._db.df_indexed.at[url, 'Processed'] = True

                # Save both tables
                self._db.save_data('df_indexed', mode='replace')
                self._db.save_data('df_processed', mode='append')
    def _process_article(self, text, url):
        doc = self.spacy(text)
        data = pd.DataFrame(
            [[[token.lemma_ for token in doc], [token.is_stop for token in doc], [token.pos_ for token in doc],
              [token.tag_ for token in doc], [token.dep_ for token in doc], [token.shape_ for token in doc]]],
            columns=['lemmatized', 'stop_words', 'pos_tags', 'tags', 'deps', 'shapes'],
            index=[url]
        )
        return data


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
