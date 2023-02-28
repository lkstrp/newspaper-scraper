"""
TODO DOCSTRING
"""
import os
import re
import datetime as dt
import locale
import signal

import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from goose3 import Goose
from tqdm import tqdm

import logger

# Declare logger
log = logger.setup_custom_logger(os.path.basename(__file__)[:-3])


def delay_interrupt(func):
    """
    TODO DOCSTRING
    """

    def wrapper(*args, **kwargs):
        s = signal.signal(signal.SIGINT, signal.SIG_IGN)
        func(*args, **kwargs)
        signal.signal(signal.SIGINT, s)

    return wrapper


class Database:
    """
    TODO DOCSTRING
    """

    @delay_interrupt
    def __init__(self, db_file="../articles.db"):
        self.db_file = db_file
        self._conn = sqlite3.connect(db_file)
        self._cur = self._conn.cursor()
        self._save_interval = 0
        self._last_save = dt.datetime.now() - dt.timedelta(seconds=self._save_interval)

        self.articles = None
        self._load_tables()

        log.info(f'Loaded {len(self.articles)} articles from database.')

    @delay_interrupt
    def _load_tables(self):
        try:
            self.articles = pd.read_sql_query(f"SELECT * FROM tblArticles",
                                              self._conn,
                                              index_col='URL',
                                              parse_dates=['PubDateIndexPage', 'DateIndexed', 'DateScrapedHTML',
                                                           'DateParsedHTML'])
            self.articles.Public = self.articles.Public.apply(lambda x: True if x == '1' or x == 1 else x)
            self.articles.Public = self.articles.Public.apply(lambda x: False if x == '0' or x == 0 else x)

        except pd.errors.DatabaseError:
            log.info(f'Database not found. Creating new database in {self.db_file}.')
            self._create_tables()

    @delay_interrupt
    def _create_tables(self):
        self._cur.execute('CREATE TABLE tblArticles ('
                          'URL TEXT PRIMARY KEY, '

                          'NewspaperID TEXT, '
                          'PubDateIndexPage DATE, '
                          'DateIndexed DATE, '

                          'Public BIT,'
                          'DateScrapedHTML DATE,'

                          'Title TEXT, '
                          'Authors TEXT, '
                          'PublishDate DATE, '
                          'Description TEXT, '
                          'CleanedText TEXT, '
                          'DateParsedHTML DATE)')

        self._cur.execute('CREATE TABLE tblRawHTML ('
                          'URL TEXT PRIMARY KEY, '
                          'RawHTML BLOP)')
        self._conn.commit()
        self._load_tables()

    @delay_interrupt
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save_articles()
        self._conn.close()
        self._cur.close()

    @delay_interrupt
    def raw_html_add_index(self, urls):
        """
        TODO DOCSTRING
        """
        for url in urls:
            self._cur.execute(
                'INSERT INTO tblRawHTML (URL, RawHTML) VALUES (?, NULL) ON CONFLICT(URL) DO UPDATE SET RawHTML=NULL',
                (url,))
        self._conn.commit()

    @delay_interrupt
    def raw_html_add_blop(self, url, raw_html):
        """
        TODO DOCSTRING
        """
        self._cur.execute('UPDATE tblRawHTML SET RawHTML=? WHERE URL=?', (raw_html, url))
        self._conn.commit()

    @delay_interrupt
    def raw_html_get_blop(self, url):
        """
        TODO DOCSTRING
        """
        return self._cur.execute('SELECT RawHTML FROM tblRawHTML WHERE URL=?', (url,)).fetchall()

    @delay_interrupt
    def save_articles(self):
        """
        TODO DOCSTRING
        """
        if self._last_save + dt.timedelta(seconds=self._save_interval) > dt.datetime.now():
            return
        # Convert lists to strings
        for column in self.articles.columns:
            if any([isinstance(x, list) for x in self.articles[column].values]):
                self.articles[column] = self.articles[column].apply(lambda x: str(x))
        self.articles.to_sql('tblArticles', self._conn, index_label='URL', if_exists='replace')


class Scraper:
    """
    TODO DOCSTRING
    """
    _db = Database()

    def __init__(self):
        self.newspaper_id = re.sub(r'(?<!^)(?=[A-Z])', '_', self.__class__.__name__).lower()

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

        parsed_infos = pd.DataFrame(
            {'Title': article.title,
             'Authors': article.authors,
             'PublishDate': article.publish_date,
             'Description': article.meta_description,
             'CleanedText': article.cleaned_text,
             'DateParsedHTML': dt.datetime.now()},
            index=[url])
        parsed_infos.index.name = 'URL'

        # parsed_infos = article.infos
        # assert parsed_infos, f'Article infos are empty.'
        # parsed_infos = pd.json_normalize(article.infos, sep='_')
        # parsed_infos.index = [url]
        # parsed_infos.index.name = 'URL'

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


class DeSpiegel(Scraper):
    """
    TODO DOCSTRING
    """

    def __init__(self):
        super().__init__()
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


if __name__ == "__main__":
    spiegel = DeSpiegel()
    spiegel.index_published_articles('2020-1-1', '2020-12-31', skip_existing=True)
    spiegel.scrape_public_articles_raw_html(parse_html=True)
