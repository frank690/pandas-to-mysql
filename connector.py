from sqlalchemy import create_engine
from contextlib import contextmanager
import pandas as pd
import numpy as np


class MissingTableError(Exception):
    """This error is raised when ever data should be handled inside a table that does not exist..."""
    pass


class Connector:
    def __init__(self, database, user, password, schema):
        """Handle initialization of this class"""
        # handle inputs
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema

        # setup database connection
        self._create_engine()
        # query some infos about the current schema
        self.tables = self._get_existing_tables()

    def insert(self, df: pd.DataFrame, table: str, key: str = None, create: bool = True):
        """Generate a no of new flights and put them into the desired table."""
        columns, data = self._exploit_dataframe(df=df, key=key)

        if table not in self.tables:
            if create:
                self.create_table(table=table, columns=columns, data=data)
            else:
                raise MissingTableError('{0} is not present in the schema {1}'.format(table, self.schema))

        # start constructing sql string
        sql = 'INSERT INTO {0} ('.format(table)
        sql += ', '.join(columns) + ') VALUES ('

        with self._cursor() as cursor:
            for dat in data:
                order = sql + ', '.join(["'" + str(d) + "'" for d in dat]) + ');'
                cursor.execute(order)

    def _create_engine(self):
        """Create an database engine"""
        # create db engine
        self.engine = create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.format(self.user,
                                                                             self.password,
                                                                             self.database,
                                                                             self.schema))

    def create_table(self, table: str, columns: list, data: list):
        """Create a new table with columns"""
        sql = 'CREATE TABLE {0}('.format(table)
        dtypes = self._determine_dtypes(data)
        for column, dtype in zip(columns, dtypes):
            sql += column + ' ' + dtype + ', '
        sql = sql[:-2] + ');'

        with self._cursor() as cursor:
            cursor.execute(sql)

    def _get_existing_tables(self) -> list:
        """Query the DB to get the names of all existing tables."""
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '{0}';".format(self.schema)
        with self._cursor() as cursor:
            tables = cursor.execute(sql)

        return [table['TABLE_NAME'] for table in tables]

    @contextmanager
    def _cursor(self):
        """Use this pilot to manage connections via engine to oracle database and commit data."""
        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                yield connection
                trans.commit()
            except Exception as e:
                trans.rollback()
                raise e

    @staticmethod
    def _determine_dtypes(data: list) -> list:
        """Determine the sql datatypes of the given data"""
        row = idx = 0
        dtypes = []
        while idx < len(data[row]):
            value = data[row][idx]
            if value is None:
                row += 1
                continue
            elif isinstance(value, np.integer):
                dtypes += ['INTEGER']
            elif isinstance(value, float):
                if np.isnan(value):
                    row += 1
                    continue
                else:
                    dtypes += ['FLOAT']
            else:  # everything is a varchar if you look hard enough!
                dtypes += ['TEXT']
            idx += 1
        return dtypes

    @staticmethod
    def _exploit_dataframe(df: pd.DataFrame, key: str = None) -> (list, list):
        """Extract columns and data from dataframe"""
        # extract data and column names from dataframe
        if key:
            columns = [key] + list(df.columns)
            data = list(df.to_records(index=True))
        else:
            columns = list(df.columns)
            data = list(df.to_records(index=False))

        return columns, data


if __name__ == '__main__':
    motorcycles = pd.read_csv('/Users/frankeschner/Documents/Projects/pandas-to-mysql/data2.csv', index_col=0)
    con = Connector('127.0.0.1', 'root', '9W7G3WGLn48zdzpPQ92Y42d9', 'ads')
    con.insert(df=motorcycles, table='ptasder', key='id', create=False)
# TODO: Autocreate missing columns (?)
# TODO: Upsert stuff
