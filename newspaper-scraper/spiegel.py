"""
TODO DOCSTRING
"""
import os
import re
import datetime as dt

import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

import logger

# Declare logger
log = logger.setup_custom_logger(os.path.basename(__file__)[:-3])


class Database:
    """
    TODO DOCSTRING
    """

    def __init__(self, db_file="../articles.db"):
        self._conn = sqlite3.connect(db_file)
        self._cur = self._conn.cursor()

        try:
            self.articles = pd.read_sql_query(f"SELECT * FROM tblArticles",
                                              self._conn,
                                              index_col='URL',
                                              parse_dates=['DateIndexPage'])
        except pd.errors.DatabaseError:
            log.info(f'Database not found. Creating new database in {db_file}.')
            self._cur.execute('CREATE TABLE tblArticles ('
                              'URL TEXT PRIMARY KEY, '
                              'NewspaperID TEXT, '
                              'DateIndexPage DATE, '
                              'Public BIT,'
                              'RawHTMLScraped BIT)')
            self._cur.execute('CREATE TABLE tblRawHTML ('
                              'URL TEXT PRIMARY KEY, '
                              'RawHTML BLOP)')

            self.articles = pd.read_sql_query(f"SELECT * FROM tblArticles", self._conn, index_col='URL')

        log.info(f'Loaded {len(self.articles)} articles from database.')
        bla = self._cur.execute('SELECT URL FROM tblRawHTML WHERE RawHTML IS NULL').fetchall()
        self.articles['RawHTMLScraped'] = ~self.articles.index.isin([x[0] for x in bla])

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    def raw_html_add_index(self, urls):
        for url in urls:
            self._cur.execute('INSERT INTO tblRawHTML (URL, RawHTML) VALUES (?, NULL)', (url,))

    def raw_html_add_blop(self, url, raw_html):
        self._cur.execute('UPDATE tblRawHTML SET RawHTML=? WHERE URL=?', (raw_html, url))

    def save_articles(self):
        self.articles.to_sql('tblArticles', self._conn, index_label='URL', if_exists='replace')


class Scraper:
    """
    TODO DOCSTRING
    """
    _db = Database()

    def __init__(self):
        self.newspaper_id = re.sub(r'(?<!^)(?=[A-Z])', '_', self.__class__.__name__).lower()

    def index_published_articles_per_day(self, date_from, date_to, skip_existing=True):
        """
        TODO DOCSTRING
        """

        date_from = pd.to_datetime(date_from)
        date_to = pd.to_datetime(date_to)

        date_range = [day for day in pd.date_range(date_from, date_to) if
                      day not in self._db.articles.DateIndexPage.values or not skip_existing]

        if len(date_range) == 0:
            log.info(f'No new days to scrape. Pass skip_existing=False to scrape all days again.')
            return

        plog = tqdm(total=0, position=0, bar_format='{desc}')
        pbar = tqdm(total=len(date_range), position=1)
        log.info(f'Start scraping articles for {len(date_range)} days. '
                 f'{len(pd.date_range(date_from, date_to)) - len(date_range)} days already indexed.')
        counter = 0
        for day in date_range:
            counter += 1
            pbar.update(1)

            newspaper, urls = self.get_published_articles(day)
            urls = [url.split('?')[0] for url in urls]

            urls = pd.DataFrame({'NewspaperID': self.newspaper_id, 'DateIndexPage': day}, index=urls)
            # Mark if urls are new
            urls['new'] = ~urls.index.isin(self._db.articles.index)

            self._db.raw_html_add_index(urls[urls['new']].index)

            self._db.articles = pd.concat([self._db.articles, urls[urls['new']].drop('new', axis=1)])

            plog.set_description_str(f'{counter}/{len(date_range)}: Indexed {urls.new.sum()}/{len(urls)} articles '
                                     f'for {day.strftime("%d.%m.%Y")}.')

            if counter % 50 == 0 or counter == len(date_range):
                self._db.save_articles()

    def scrape_public_articles_raw_html(self):
        """
        TODO DOCSTRING
        """
        to_scrape = self._db.articles[(self._db.articles.NewspaperID == self.newspaper_id)
                                      & (self._db.articles.Public.isnull())]

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
                plog.set_description_str(f'{counter}/{len(to_scrape)}: Scraped article. '
                                         f'(Stats: {stats.count(1)}/{stats.count(0)} '
                                         f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')
                self._db.raw_html_add_blop(url, raw_html)
                stats.append(1)
            else:
                plog.set_description_str(f'{counter}/{len(to_scrape)}: Article is premium. '
                                         f'(Stats: {stats.count(1)}/{stats.count(0)} '
                                         f'{stats[-100:].count(1)}/{stats[-100:].count(0)}).')
                stats.append(0)

            self._db.articles.loc[url, 'Public'] = public

            if counter % 50 == 0 or counter == len(to_scrape):
                self._db.save_articles()

    def get_published_articles(self, day):
        raise NotImplemented

    def get_raw_html(self, url):
        raise NotImplemented


class DeSpiegel(Scraper):
    def __init__(self):
        super().__init__()

    def get_published_articles(self, day):
        URL = f'https://www.spiegel.de/nachrichtenarchiv/artikel-{day.strftime("%d.%m.%Y")}.html'

        soup = BeautifulSoup(requests.get(URL).content, "html.parser")
        urls = soup.find("section", {"data-area": "article-teaser-list"}).find_all("div",
                                                                                   {"data-block-el": "articleTeaser"})
        urls = [article.find('a')['href'] for article in urls]

        return 'de-spiegel', urls

    def get_raw_html(self, url):
        html = requests.get(url).content
        soup = BeautifulSoup(html, "html.parser")
        premium_icon = soup.find("header", {"data-area": "intro"}).find('svg', {"id": "spon-spplus-flag-l"})

        return html, not bool(premium_icon)


if __name__ == "__main__":
    spiegel = DeSpiegel()
    spiegel.index_published_articles_per_day('2020-1-1', dt.datetime.today())
    spiegel.scrape_public_articles_raw_html()
