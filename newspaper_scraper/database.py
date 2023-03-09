"""
TODO DOCSTRING
"""
import signal
import datetime as dt
import os

import pandas as pd
import sqlite3

from .utils.logger import CustomLogger

# Declare logger
log = CustomLogger(os.path.basename(__file__)[:-3], log_file='logs.log')


def delay_interrupt(func):
    """
    This decorator is used to handle keyboard interrupts. It is applied to methods that perform long-running operations
     which should not be interrupted (like writings to the SQL database). It ignores keyboard interrupts while the
     function is running and then restores the interrupt handler when it is finished.

    Args:
        func (function): The function to be wrapped.
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
    def __init__(self, db_file, save_interval=60):
        self.db_file = db_file
        self.articles = None

        self._save_interval = save_interval
        self._last_save = dt.datetime.now() - dt.timedelta(seconds=self._save_interval)

        self._cur = None
        self._conn = None

    def connect(self):
        """
        TODO DOCSTRING
        """
        self._conn = sqlite3.connect(self.db_file)
        self._cur = self._conn.cursor()
        self.articles = None
        self._load_tables()

    def _load_tables(self):
        log.info(f'Loading database from {self.db_file}.')
        try:
            self.articles = pd.read_sql_query(f"SELECT * FROM tblArticles",
                                              self._conn,
                                              index_col='URL',
                                              parse_dates=['PubDateIndexPage', 'DateIndexed', 'DateScrapedHTML',
                                                           'DateParsedHTML'])
            self.articles.Public = self.articles.Public.apply(lambda x: True if x == '1' or x == 1 else x)
            self.articles.Public = self.articles.Public.apply(lambda x: False if x == '0' or x == 0 else x)

            log.info(f'Found {len(self.articles)} articles in database.')

        except pd.errors.DatabaseError:
            log.info(f'No Database found. Creating new database.')
            self._create_tables()

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
                          'PublishDate TEXT, '
                          'MetaDescription TEXT, '
                          'Domain TEXT, '
                          'OpengraphTitle TEXT, '
                          'OpengraphType TEXT, '
                          'OpengraphUrl TEXT, '
                          'OpengraphImage TEXT, '
                          'OpengraphDescription TEXT, '
                          'OpengraphSiteName TEXT, '
                          'CleanedText TEXT, '
                          'MetaLang TEXT, '
                          'MetaKeywords TEXT, '
                          'MetaFavicon TEXT, '
                          'MetaCanonical TEXT, '
                          'MetaEncoding TEXT, '
                          'Image TEXT, '
                          'Tags TEXT, '
                          'Tweets TEXT, '
                          'Movies TEXT, '
                          'Links TEXT)')

        self._load_tables()

    @delay_interrupt
    def save_articles(self):
        """
        TODO DOCSTRING
        """
        if self._last_save + dt.timedelta(seconds=self._save_interval) > dt.datetime.now():
            return

        df = self.articles.copy()

        # Convert lists to strings
        for column in df.columns:
            if any([isinstance(x, list) for x in df[column].values]):
                df[column] = df[column].apply(lambda x: str(x))

        # Convert dates to strings
        df.PubDateIndexPage = df.PubDateIndexPage.apply(str)

        df.to_sql('tblArticles', self._conn, index_label='URL', if_exists='replace')

    @delay_interrupt
    def close(self):
        """
        TODO DOCSTRING
        """
        self.save_articles()
        self._cur.close()
        self._conn.close()
