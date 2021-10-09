from .connector import Connector
from pandas import DataFrame


class HiveConnector(Connector):
    _catalog = 'hive'
    _presto = {
        'host': 'presto',
        'port': 8080,
    }

    def __init__(self: object):
        super(HiveConnector, self).__init__(HiveConnector._catalog)

    def __del__(self: object):
        pass

    def _insert_df(self: object, schema: str, table: str, df: DataFrame):
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import to_sql

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveConnector._presto['host'],
             HiveConnector._presto['port'],
             schema))

        to_sql(df, table, engine, if_exists='append',
               index=False, index_label=None, chunksize=None,
               dtype=None, method='multi')

    def _insert_sql(self: object, schema: str, sql: str):
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import execute

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveConnector._presto['host'],
             HiveConnector._presto['port'],
             schema))

        execute(sql, engine)

    def _delete_list(self: object, schema: str, table: str, conditions: list[str]):
        from sqlalchemy import MetaData, Table, delete
        from sqlalchemy.sql.expression import text
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import execute

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveConnector._presto['host'],
             HiveConnector._presto['port'],
             schema))
        metadata = MetaData(bind=engine)
        user_table = Table(
            table, metadata, autoload=True, autoload_with=engine)

        sql = delete(user_table)
        for condition in conditions:
            sql = sql.where(text(condition))

        execute(sql, engine)

    def _delete_dict(self: object, schema: str, table: str, conditions: dict[str, str]):
        from sqlalchemy import MetaData, Table, delete
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import execute

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveConnector._presto['host'],
             HiveConnector._presto['port'],
             schema))
        metadata = MetaData(bind=engine)
        user_table = Table(
            table, metadata, autoload=True, autoload_with=engine)

        sql = delete(user_table)
        for key, value in conditions.items():
            sql = sql.where(user_table.columns[key] == value)

        execute(sql, engine)

    def _select_dict(self: object, schema: str, table: str,  conditions: dict[str, str]) -> DataFrame:
        from sqlalchemy import MetaData, Table, select
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import read_sql

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveConnector._presto['host'],
             HiveConnector._presto['port'],
             schema))
        metadata = MetaData(bind=engine)
        user_table = Table(table, metadata, autoload=True,
                           autoload_with=engine)

        sql = select(user_table)
        for key, value in conditions.items():
            sql = sql.where(user_table.columns[key] == value)

        return read_sql(sql, engine)

    def _select_sql(self: object, schema: str, sql: str):
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import read_sql

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveConnector._presto['host'],
             HiveConnector._presto['port'],
             schema))

        return read_sql(sql, engine)
