"""
TODO DOCSTRING
"""
import os
import re
import datetime as dt

import time


import pandas as pd
from goose3 import Goose
from tqdm import tqdm

from .utils.logger import setup_custom_logger
from .database import Database

# Declare logger
log = setup_custom_logger(os.path.basename(__file__)[:-3])


class Scraper:
    """
    TODO DOCSTRING
    """
    _db = Database()

    def __init__(self, username, password):
        self.usr = username
        self.psw = password

        self.newspaper_id = re.sub(r'(?<!^)(?=[A-Z])', '_', self.__class__.__name__).lower()
        self.selenium_driver = None

    def index_published_articles(self, date_from, date_to, skip_existing=True):
        """
        TODO DOCSTRING
        """

        if date_from:
            date_from = pd.to_datetime(date_from)
            date_to = pd.to_datetime(date_to)

            date_range = [day for day in pd.date_range(date_from, date_to) if
                          not any([day.date() == index_day.date() for index_day in self._db.articles.PubDateIndexPage])
                          or not skip_existing]

            if len(date_range) == 0:
                log.info(f'No new days to scrape. Pass skip_existing=False to scrape all days again.')
                return

            log.info(f'Start scraping articles for {len(date_range)} days. '
                     f'{len(pd.date_range(date_from, date_to)) - len(date_range)} days already indexed.')
            plog = tqdm(total=0, position=0, bar_format='{desc}')
            pbar = tqdm(total=len(date_range), position=1)
            counter = 0
            for day in date_range:
                counter += 1
                pbar.update(1)

                urls, pub_dates = self.get_published_articles(day)

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

                # Add new urls to RawHTML table as index
                self._db.raw_html_add_index(urls[urls['new']].index)
                # Add new urls to articles table
                self._db.articles = pd.concat([self._db.articles, urls[urls['new']].drop('new', axis=1)])

                plog.set_description_str(f'{counter}/{len(date_range)}: Indexed {urls.new.sum()}/{len(urls)} articles '
                                         f'for {day.strftime("%d.%m.%Y")}.')

                self._db.save_articles()

    def scrape_public_articles_raw_html(self, parse_html=False):
        """
        TODO DOCSTRING
        """
        to_scrape = self._db.articles[(self._db.articles.NewspaperID == self.newspaper_id) &
                                      (self._db.articles.Public.isnull())]

        if to_scrape.empty:
            log.info(f'No articles to scrape.')
            return

        log.info(f'Start scraping {len(to_scrape)} articles.')
        counter = 0
        plog = tqdm(total=0, position=0, bar_format='{desc}')
        pbar = tqdm(total=len(to_scrape), position=1)

        stats = []
        for url, row in to_scrape.iterrows():
            counter += 1
            pbar.update(1)
            raw_html, public = self.get_raw_html(url)

            if public:
                plog.set_description_str(f'{counter}/{len(to_scrape)}: Article scraped. '
                                         f'(Stats: {stats.count(1)}/{stats.count(0)} '
                                         f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')
                if not parse_html:
                    self._db.raw_html_add_blop(url, raw_html)
                else:
                    results = self._parse_article(raw_html, url)
                    self._db.articles.update(results)
                    self._db.articles.loc[url, 'DateScrapedHTML'] = dt.datetime.now()

                stats.append(1)
            else:
                plog.set_description_str(f'{counter}/{len(to_scrape)}: Article is not public. '
                                         f'(Stats: {stats.count(1)}/{stats.count(0)} '
                                         f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')
                stats.append(0)

            self._db.articles.loc[url, 'Public'] = public

            self._db.save_articles()

    def scrape_private_articles_raw_html(self, parse_html=False, catch_exceptions=True):
        """
        TODO DOCSTRING
        """

        def _func():
            if self.usr is None or self.psw is None:
                raise ValueError('No username or password provided.')

            to_scrape = self._db.articles[(self._db.articles.NewspaperID == self.newspaper_id) &
                                          (self._db.articles.Public == 0) &
                                          (self._db.articles.DateScrapedHTML.isnull())]

            if to_scrape.empty:
                log.info(f'No articles to scrape.')
                return

            self.selenium_login()

            log.info(f'Start scraping {len(to_scrape)} articles.')
            counter = 0
            plog = tqdm(total=0, position=0, bar_format='{desc}')
            pbar = tqdm(total=len(to_scrape), position=1)
            for url, row in to_scrape.iterrows():
                counter += 1
                pbar.update(1)
                raw_html = self.get_private_article_raw_html(url)

                plog.set_description_str(f'{counter}/{len(to_scrape)}: Article scraped.')
                if not parse_html:
                    self._db.raw_html_add_blop(url, raw_html)
                else:
                    results = self._parse_article(raw_html, url)
                    self._db.articles.update(results)
                    self._db.articles.loc[url, 'DateScrapedHTML'] = dt.datetime.now()

                self._db.save_articles()

        while True:
            try:
                _func()
                break
            except Exception as e:
                if catch_exceptions:
                    log.exception(f'Exception while scraping private articles: {e}.')
                    log.info('Retrying in 100 seconds...')
                    time.sleep(100)
                else:
                    raise e

    def parse_raw_html(self):
        """
        TODO DOCSTRING
        """
        # TODO Needs to be implemented
        to_scrape = self._db.articles[(self._db.articles.NewspaperID == self.newspaper_id) &
                                      (self._db.articles.Public.isnull())]
        pass

    @staticmethod
    def _parse_article(html, url):
        """
        TODO DOCSTRING
        """
        g = Goose()
        article = g.extract(raw_html=html)

        try:
            parsed_infos = pd.DataFrame(
                {'Title': article.title,
                 'Authors': article.authors,
                 'PublishDate': article.publish_date,
                 'Description': article.meta_description,
                 'CleanedText': article.cleaned_text,
                 'DateParsedHTML': dt.datetime.now()},
                index=[url])
            parsed_infos.index.name = 'URL'
        except Exception as e:
            log.error(f'Error while parsing article {url}: {e}')
            parsed_infos = pd.DataFrame(index=[url])
            parsed_infos.index.name = 'URL'

        return parsed_infos

    def get_published_articles(self, day):
        """
        TODO DOCSTRING
        """
        raise NotImplemented

    def get_raw_html(self, url):
        """
        TODO DOCSTRING
        """
        raise NotImplemented

    def selenium_login(self):
        """
        TODO DOCSTRING
        """
        raise NotImplemented

    def get_private_article_raw_html(self, url):
        """
        TODO DOCSTRING
        """
        raise NotImplemented
