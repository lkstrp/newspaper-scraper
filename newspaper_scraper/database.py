import signal
import datetime as dt
import os

import pandas as pd
import sqlite3

from .utils.logger import setup_custom_logger

# Declare logger
log = setup_custom_logger(os.path.basename(__file__)[:-3])

def delay_interrupt(func):
    """
    TODO DOCSTRING
    """

    def _wrapper(*args, **kwargs):
        s = signal.signal(signal.SIGINT, signal.SIG_IGN)
        func(*args, **kwargs)
        signal.signal(signal.SIGINT, s)

    return _wrapper


class Database:
    """
    TODO DOCSTRING
    """

    @delay_interrupt
    def __init__(self, db_file="./articles.db"):
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