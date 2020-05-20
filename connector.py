from sqlalchemy import create_engine
from contextlib import contextmanager
import pandas as pd


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

    def insert(self, df: pd.DataFrame, table: str, key: str = None, **kwargs):
        """Generate a no of new flights and put them into the desired table."""
        columns, data = self._exploit_dataframe(df=df, key=key)

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
    def _exploit_dataframe(df: pd.DataFrame, key: str = None):
        """Extract columns and data from dataframe"""
        # extract data and column names from dataframe
        if key:
            columns = [key] + list(df.columns)
            data = list(df.to_records(index=True))
        else:
            columns = list(df.columns)
            data = list(df.to_records(index=False))

        return columns, data