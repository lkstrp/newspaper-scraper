"""
This module contains the database class. It is used to store the scraped articles in a SQLite database.
"""

import datetime as dt

import pandas as pd
import sqlite3

from .utils.logger import CustomLogger
from .utils.utils import delay_interrupt
from .settings import settings

# Declare logger
log = CustomLogger('newspaper-scraper', log_file=settings.log_file)


class Database:
    """
    This class is used to store the scraped articles in a SQLite database. All articles are stored in a pandas DataFrame and can 
    """

    @delay_interrupt
    def __init__(self, db_file):
        self.db_file = db_file
        self.articles = None

        self._last_save = dt.datetime.now() - dt.timedelta(seconds=settings.save_interval)

        self._cur = None
        self._conn = None

    def connect(self):
        """
        Connect to the database and load the articles table. If the database does not exist, it will be created.
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

            log.info(f'Found {len(self.articles):,} articles in database.')

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
                          'DateScrapedHTML DATE)')

        self._load_tables()

    @delay_interrupt
    def save_articles(self):
        """
        Save the articles DataFrame to the database. This method has a built-in delay to prevent saving the database too often. The delay is set in the settings file.
        """
        if self._last_save + dt.timedelta(seconds=settings.save_interval) > dt.datetime.now():
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
        Close the database connection and save the articles DataFrame.
        """
        self.save_articles()
        self._cur.close()
        self._conn.close()
