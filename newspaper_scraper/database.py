"""
This module contains the database class. It is used to store the scraped articles in a SQLite database.
"""

import datetime as dt
import time

import pandas as pd
import sqlite3

from .utils.logger import CustomLogger
from .utils.utils import delay_interrupt
from .settings import settings

# Declare logger
log = CustomLogger('newspaper-scraper', log_file=settings.log_file)


class Database:
    """
    todo docstring
    """

    @delay_interrupt
    def __init__(self, db_file):
        self.db_file = db_file
        self._df_scraped = None  # Only helper for property self.df_scraped

        # Is defined in self.connect()
        self._cur = None
        self._conn = None
        self.df_indexed = None
        self.df_scraped_cols = None
        self.df_scraped_new = None

        self._last_save = {'df_indexed': dt.datetime.now(),
                           'df_scraped': dt.datetime.now()}

    @delay_interrupt
    def connect(self):
        """
        Connect to the database and load the articles table. If the database does not exist, it will be created.
        """
        self._conn = sqlite3.connect(self.db_file)
        self._cur = self._conn.cursor()
        self.df_indexed = self._load_table('tblArticlesIndexed')
        try:
            self.df_scraped_cols = pd.read_sql_query("select * from tblArticlesScraped where URL='x'",
                                                     self._conn).columns.tolist()
        except pd.errors.DatabaseError:
            self.df_scraped_cols = []
        # Create dataframe for new scraped articles (will be appended to the database, contains only new articles)
        self.df_scraped_new = pd.DataFrame(columns=self.df_scraped_cols)
        self.df_scraped_new = self.df_scraped_new.drop('URL', axis=1, errors='ignore')
        self.df_scraped_new.index.name = 'URL'

    @delay_interrupt
    def close(self):
        """
        Close the database connection and save the articles DataFrame.
        """
        self.save_data('df_indexed', mode='replace', force=True)
        self.save_data('df_scraped', mode='append', force=True)

        self._cur.close()
        self._conn.close()

    @property
    def df_scraped(self):
        """
        Return the scraped articles DataFrame. If it is not loaded yet, it will be loaded from the database. This may
        take a while.
        """
        if self._df_scraped is None:
            self._df_scraped = self._load_table('tblArticlesScraped')
        return self._df_scraped

    def _load_table(self, table_name):
        if table_name == 'tblArticlesIndexed':
            log.info(f'Load database from {self.db_file}.')

            try:
                _df = pd.read_sql_query(f"SELECT * FROM tblArticlesIndexed",
                                        self._conn,
                                        index_col='URL',
                                        parse_dates=['PubDateIndexPage', 'DateIndexed'])
                already_parsed = pd.read_sql_query("SELECT URL FROM tblArticlesScraped", self._conn)
                _df['Scraped'] = _df.index.isin(already_parsed['URL'])

                # Handle boolean values
                _df.Scraped = _df.Scraped.apply(lambda x: True if x == '1' or x == 1 else x)
                _df.Scraped = _df.Scraped.apply(lambda x: True if x == '1' or x == 1 else x)
                _df.Public = _df.Public.apply(lambda x: True if x == '1' or x == 1 else x)
                _df.Public = _df.Public.apply(lambda x: False if x == '0' or x == 0 else x)

                log.info(f'Loaded {len(_df):,} articles from database. {len(_df[_df.Scraped]):,}/{len(_df):,} '
                         f'already scraped ({len(_df[_df.Scraped]) / len(_df) * 100:.2f}%).')
            except pd.errors.DatabaseError:
                _df = pd.DataFrame(columns=['NewspaperID', 'PubDateIndexPage', 'DateIndexed', 'Public', 'Scraped'])
                _df.index.name = 'URL'
                log.info(f'No database found. Created new database.')

            _df = _df.convert_dtypes()

        elif table_name == 'tblArticlesScraped':
            _df = pd.read_sql_query(f"SELECT * FROM tblArticlesScraped",
                                    self._conn,
                                    index_col='URL',
                                    parse_dates=['DateScrapedHTML', 'PublishDate'])

        else:
            raise ValueError('Table name not recognized.')
        return _df

    @delay_interrupt
    def save_data(self, data_name: str, mode: str, force: bool = False):
        """
        Save the articles DataFrame to the database. This method has a built-in delay to prevent saving the database
        too often. The delay is set in the settings file.
        """
        if mode not in ['append', 'replace']:
            raise ValueError(f'Mode {mode} not recognized. Mode must be "append" or "replace".')

        # Check if the save interval has passed otherwise skip saving
        if self._last_save[data_name] + dt.timedelta(seconds=settings.save_interval) > dt.datetime.now() \
                and not force:
            return
        else:
            self._last_save[data_name] = dt.datetime.now()

        if data_name == 'df_indexed':
            _df = self.df_indexed.copy()

            # Convert dates to strings
            _df.PubDateIndexPage = _df.PubDateIndexPage.apply(str)

            _df.to_sql('tblArticlesIndexed', self._conn, index_label='URL', if_exists=mode)

        elif data_name == 'df_scraped':
            if mode == 'append':
                _df = self.df_scraped_new.copy()
                self.df_scraped_new = self.df_scraped_new.head(0)
            else:
                _df = self.df_scraped.copy()

            if _df.empty:
                return

            # Convert lists to strings
            for column in _df.columns:
                if any([isinstance(x, list) for x in _df[column].values]):
                    _df[column] = _df[column].apply(lambda x: str(x))

            _df.to_sql('tblArticlesScraped', self._conn, index_label='URL', if_exists=mode)
