"""
This module contains the NewspaperManager class which is used to scrape articles from a newspaper website. It is only
used as a base class for the actual scrapers.
"""

import re
import datetime as dt

import numpy as np
import pandas as pd
from goose3 import Goose
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

try:
    import spacy
except ImportError:
    spacy = None

from .utils.logger import log
from .settings import settings
from .constants import NLP_ATTRIBUTES
from .utils.utils import flatten_dict
from .utils.utils import get_selenium_webdriver
from .utils.utils import retry_on_exception
from .database import Database


class NewspaperManager:
    """
    The NewspaperManager class is used to scrape articles from a newspaper website. It is only used as a base class for
    the actual scrapers and can not be instantiated directly. Each newspaper scraper needs to inherit from this class
    and implement the _get_published_articles, _soup_get_html, and _selenium_login methods.
    
    It contains the following methods:
        - index_published_articles: Scrapes all articles published between date_from and date_to.
        - scrape_public_articles: Checks for all indexed if they are public and scrapes them if they are.
        - scrape_premium_articles: Scrapes all private articles. Needs a valid login.
        - _parse_article: Parses the HTML of an article and returns a DataFrame with the parsed infos.
        - _get_published_articles: Not implemented. Needs to be implemented in the actual scraper.
        - _soup_get_html: Not implemented. Needs to be implemented in the actual scraper.
        - _selenium_login: Not implemented. Needs to be implemented in the actual scraper.
    """

    def __init__(self, db_file):
        self.newspaper_id = re.sub(r'(?<!^)(?=[A-Z])', '_', self.__class__.__name__).lower()
        log.info(f'Initializing {self.newspaper_id} scraper.')

        self._db = Database(db_file=db_file)
        self._selenium_driver = None
        self._spacy_nlp = None

    @property
    def selenium_driver(self):
        """
        Returns the selenium driver. If no driver is set, it tries to get one from the settings file. If this fails, it
        tries to get one from the utils.utils.get_selenium_webdriver function.
        """
        if self._selenium_driver is None:
            if settings.selenium_driver:
                self._selenium_driver = settings.selenium_driver
            else:
                try:
                    self._selenium_driver = get_selenium_webdriver()
                except Exception:
                    raise Exception(f'Could not get selenium webdriver. Please set a selenium_driver property via '
                                    f'newspaper_scraper.settings.selenium_driver = <driver>.')
        return self._selenium_driver

    @property
    def spacy_nlp(self, model='de_core_news_sm'):
        """
        Returns the spacy nlp model. If no model is set, it loads the model from spacy.
        """
        if spacy is None:
            raise ImportError('To use the nlp functionality, additional dependencies need to be installed. Please run '
                              '"pip install newspaper_scraper[nlp]" to install them.')
        if self._spacy_nlp is None:
            try:
                self._spacy_nlp = spacy.load(model)
            except OSError:
                raise OSError(f'Could not load spacy model "{model}". Please download it via "python -m spacy download '
                              f'{model}".')
        return self._spacy_nlp

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
    def index_articles_by_date_range(self, date_from, date_to, skip_existing=True):
        """
        Indexes all articles published between date_from and date_to for a given newspaper. Indexing means that the
        articles are added to the database and their URLs are stored. The actual scraping of the articles is done
        separately with the scrape_public_articles and scrape_premium_articles methods.

        Args:
            date_from (dt.datetime or str): The first day to scrape articles from.
            date_to (dt.datetime or str): The last day to scrape articles from.
            skip_existing (bool, optional): If True, days that are already indexed are skipped. Defaults to True.
        """
        date_from = pd.to_datetime(date_from)
        date_to = pd.to_datetime(date_to)

        already_indexed = self._db.df_indexed[(self._db.df_indexed.NewspaperID == self.newspaper_id)]

        # Convert already_indexed.PubDateIndexPage to set of dates for faster lookup
        indexed_dates = {index_day.date() for index_day in already_indexed.PubDateIndexPage}
        # Create pd.date_range and convert it to a numpy array of dates for faster lookup
        date_range = pd.date_range(date_from, date_to)
        date_range_dates = np.array([day.date() for day in date_range])

        if skip_existing:
            date_range = date_range[~np.in1d(date_range_dates, list(indexed_dates))]
        else:
            date_range = date_range.tolist()

        if len(date_range) == 0:
            log.info(f'No new days to scrape. Pass skip_existing=False to scrape all days again.')
            return

        log.info(f'Start scraping articles for {len(date_range):,} days ({date_from.strftime("%d.%m.%y")} - '
                 f'{date_to.strftime("%d.%m.%y")}). {len(pd.date_range(date_from, date_to)) - len(date_range):,} '
                 f'days already indexed.')
        counter = 0
        with logging_redirect_tqdm(loggers=[log]):
            for day in tqdm(date_range):
                counter += 1

                urls, pub_dates = self._get_articles_by_date(day)

                assert all([isinstance(pub_date, dt.datetime) for pub_date in pub_dates]), \
                    f'Not all pub_dates are datetime objects.'
                assert all([pub_date.tzinfo is not None for pub_date in pub_dates]), \
                    f'Not all pub_dates contain timezone info.'

                # Convert pub_dates to UTC
                pub_dates = [pub_date.astimezone(dt.timezone.utc) for pub_date in pub_dates]
                # Remove timezone info from pub_dates
                pub_dates = [pub_date.replace(tzinfo=None) for pub_date in pub_dates]

                # Remove query strings from urls
                urls = [url.split('?')[0] for url in urls]

                urls = pd.DataFrame({'NewspaperID': self.newspaper_id,
                                     'PubDateIndexPage': pub_dates,
                                     'DateIndexed': dt.datetime.now(),
                                     'Public': None,
                                     'Scraped': False,
                                     'Processed': False},
                                    index=urls)
                # Mark if urls are new
                urls['new'] = ~urls.index.isin(self._db.df_indexed.index).astype(bool)

                # Add new urls to articles table
                self._db.df_indexed = pd.concat([self._db.df_indexed, urls[urls['new']].drop('new', axis=1)])
                log.info(f'{counter}/{len(date_range)}: Indexed {urls.new.sum()}/{len(urls)} articles '
                         f'for {day.strftime("%d.%m.%Y")} (n={len(self._db.df_indexed):,}).')

                self._db.save_data('df_indexed', mode='replace')

    @retry_on_exception
    def index_articles_by_edition(self, edition_from: str, edition_to: str, editions_per_year=55, skip_existing=True):
        """
        Index all articles published in between two editions for a given newspaper. Indexing means that the
        articles are added to the database and their URLs are stored. The actual scraping of the articles is done
        separately with the scrape_public_articles and scrape_premium_articles methods.

        Args:
            edition_from (str): The first edition to scrape articles from. The format must be 'YYYY-EDITION'
                (e.g. '2019-1').
            edition_to (str): The last edition to scrape articles from. The format must be 'YYYY-EDITION'
                (e.g. '2019-1').
            editions_per_year (int, optional): The number of editions per year. Defaults to 55. If edtions across
                multiple years are scraped, this number is used for all years in between.
            skip_existing (bool, optional): If True, days that are already indexed are skipped. Defaults to True.
        """
        if re.match(r'\d{4}-(?!0)\d{1,2}', edition_from) is None:
            raise ValueError(f'edition_from must be in the format "YYYY-EDITION" (e.g. "2019-1").')
        if re.match(r'\d{4}-(?!0)\d{1,2}', edition_to) is None:
            raise ValueError(f'edition_to must be in the format "YYYY-EDITION" (e.g. "2019-1").')

        year_from = int(edition_from.split('-')[0])
        year_to = int(edition_to.split('-')[0])

        edition_from = int(edition_from.split('-')[1])
        edition_to = int(edition_to.split('-')[1])

        edition_range = []
        for year in range(year_from, year_to + 1):
            if year == year_from and year == year_to:
                editions = range(edition_from, edition_to + 1)
            elif year == year_from:
                editions = range(edition_from, editions_per_year + 1)
            elif year == year_to:
                editions = range(1, edition_to + 1)
            else:
                editions = range(1, editions_per_year + 1)

            for edition in editions:
                edition_range.append(f'{year}-{edition}')

        already_indexed = self._db.df_indexed[(self._db.df_indexed.NewspaperID == self.newspaper_id)]
        indexed_editions = [edition for edition in already_indexed.Edition.unique().tolist()]

        if skip_existing:
            edition_range = [edition for edition in edition_range if edition not in indexed_editions]

        if len(edition_range) == 0:
            log.info(f'No new editions to scrape. Pass skip_existing=False to scrape all days again.')
            return

        log.info(f'Start scraping articles for {len(edition_range):,} editions ({year_from}-{edition_from} - '
                 f'{year_to}-{edition_to}).')  # todo add already indexed editions

        counter = 0
        with logging_redirect_tqdm(loggers=[log]):
            for edition in tqdm(edition_range, total=len(edition_range)):
                counter += 1
                year = int(edition.split('-')[0])
                edition = int(edition.split('-')[1])

                urls = self._get_articles_by_edition(year, edition)

                # Remove query strings from urls
                urls = [url.split('?')[0] for url in urls]

                urls = pd.DataFrame({'NewspaperID': self.newspaper_id,
                                     'Edition': f'{year}-{edition}',
                                     'DateIndexed': dt.datetime.now(),
                                     'Public': None,
                                     'Scraped': False,
                                     'Processed': False},
                                    index=urls)
                # Mark if urls are new
                urls['new'] = ~urls.index.isin(self._db.df_indexed.index).astype(bool)

                # Add new urls to articles table
                self._db.df_indexed = pd.concat([self._db.df_indexed, urls[urls['new']].drop('new', axis=1)])
                log.info(f'{counter}/{len(edition_range)}: Indexed {urls.new.sum()}/{len(urls)} articles '
                         f'for {year}-{edition} (n={len(self._db.df_indexed):,}).')

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

        log.info(f'Start scraping {len(to_scrape):,} public articles.')
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
                        if col not in self._db.df_scraped_new.columns:
                            log.info(f'New feature detected: {col}')

                    # Add results to articles table
                    self._db.df_scraped_new = pd.concat([self._db.df_scraped_new, results], axis=0)
                    self._db.df_scraped_new.loc[url, 'DateScrapedHTML'] = dt.datetime.now()
                    self._db.df_indexed.at[url, 'Scraped'] = True

                    stats.append(1)
                    # log.info(f'{counter}/{len(to_scrape)}: Article scraped. '
                    #          f'(Stats: {stats.count(1)}/{stats.count(0)} '
                    #          f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')
                    # todo allow setting for more detailed prints

                else:
                    stats.append(0)
                    # log.info(f'{counter}/{len(to_scrape)}: Article is not public. '
                    #          f'(Stats: {stats.count(1)}/{stats.count(0)} '
                    #          f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')
                    # todo allow setting for more detailed prints

                # Add public information and save both tables
                self._db.df_indexed.at[url, 'Public'] = public
                self._db.save_data('df_indexed', mode='replace')
                self._db.save_data('df_scraped', mode='append')

    @retry_on_exception
    def scrape_premium_articles(self, username: str, password: str):
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

        # Reset selenium driver
        if self._selenium_driver is not None:
            self._selenium_driver.close()
        self._selenium_driver = None
        # Login
        login_successful = self._selenium_login(username=username, password=password)
        if not login_successful:
            log.warning(f'Login failed. Skip scraping.')
            return

        # Scrape articles
        log.info(f'Start scraping {len(to_scrape)} premium articles.')
        counter = 0
        with logging_redirect_tqdm(loggers=[log]):
            for url, row in tqdm(to_scrape.iterrows(), total=len(to_scrape)):
                counter += 1

                # Scrape article
                raw_html = self._selenium_get_html(url)
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
        Processes all scraped articles, which have not been processed yet. Uses spacy to process the articles.
        """
        to_process = self._db.df_scraped[(self._db.df_indexed.reindex(self._db.df_scraped.index).NewspaperID ==
                                          self.newspaper_id) &
                                         (self._db.df_indexed.reindex(self._db.df_scraped.index).Processed != True)]

        # Return if no articles to scrape
        if to_process.empty:
            log.info(f'No articles to process.')
            return

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
        doc = self.spacy_nlp(text)
        data = pd.DataFrame(
            [[np.array([token.__getattribute__(attribute) for token in doc]) for attribute in NLP_ATTRIBUTES]],
            columns=NLP_ATTRIBUTES,
            index=[url]
        )
        data['left_edge'] = data['left_edge'].apply(lambda x: np.array([token.i for token in x]))
        data['right_edge'] = data['right_edge'].apply(lambda x: np.array([token.i for token in x]))
        data['morph'] = data['morph'].apply(lambda x: np.array([token.to_dict() for token in x]))

        return data

    def _get_articles_by_date(self, day):
        """
        Exists only as a placeholder. Needs to be implemented by the child class for each newspaper.
        """
        raise NotImplemented

    def _get_articles_by_edition(self, year, edition):
        """
        Exists only as a placeholder. Needs to be implemented by the child class for each newspaper.
        """
        raise NotImplemented

    def _soup_get_html(self, url):
        """
        Exists only as a placeholder. Needs to be implemented by the child class for each newspaper.
        """
        raise NotImplemented

    def _selenium_get_html(self, url):
        """
        Function to get the html of a private article. Uses selenium to get the html. Can be optionally overwritten by
        the child if a different method is needed.
        """
        self.selenium_driver.get(url)
        return self.selenium_driver.page_source

    def _selenium_login(self, username: str, password: str):
        """
        Exists only as a placeholder. Needs to be implemented by the child class for each newspaper.
        """
        raise NotImplemented
