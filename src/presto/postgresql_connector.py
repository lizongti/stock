from .connector import Connector
from pandas import DataFrame


class PostgresqlConnector(Connector):
    _catalog = 'postgresql'
    _postgresql = {
        'host': 'postgres',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres',
        'database': 'postgres',
    }
    _presto = {
        'host': 'presto',
        'port': 8080,
    }

    def __init__(self: object):
        super(PostgresqlConnector, self).__init__(PostgresqlConnector._catalog)

    def __del__(self: object):
        pass

    def _insert_df(self: object, schema: str, table: str, df: DataFrame):
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import to_sql

        engine = create_engine('presto://%s:%d/postgresql/%s' %
                               (PostgresqlConnector._presto['host'],
                                PostgresqlConnector._presto['port'],
                                schema))

        to_sql(df, table, engine, if_exists='append',
               index=False, index_label=None, chunksize=None,
               dtype=None, method='multi')

    def _delete_list(self: object, schema: str, table: str, conditions: list[str]):
        from sqlalchemy import MetaData, Table, delete
        from sqlalchemy.sql.expression import text
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import execute

        engine = create_engine('postgresql+psycopg2://%s:%s@%s:%d/%s' %
                               (PostgresqlConnector._postgresql['user'],
                                PostgresqlConnector._postgresql['password'],
                                PostgresqlConnector._postgresql['host'],
                                PostgresqlConnector._postgresql['port'],
                                PostgresqlConnector._postgresql['database']))
        metadata = MetaData(bind=engine)
        user_table = Table(
            table, metadata, schema=schema, autoload=True, autoload_with=engine)

        sql = delete(user_table)
        for condition in conditions:
            sql = sql.where(text(condition))

        execute(sql, engine)

    def _delete_dict(self: object, schema: str, table: str, conditions: dict[str, str]):
        from sqlalchemy import MetaData, Table, delete
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import execute

        engine = create_engine('postgresql+psycopg2://%s:%s@%s:%d/%s' %
                               (PostgresqlConnector._postgresql['user'],
                                PostgresqlConnector._postgresql['password'],
                                PostgresqlConnector._postgresql['host'],
                                PostgresqlConnector._postgresql['port'],
                                PostgresqlConnector._postgresql['database']))
        metadata = MetaData(bind=engine)
        user_table = Table(
            table, metadata, schema=schema, autoload=True, autoload_with=engine)

        sql = delete(user_table)
        for key, value in conditions.items():
            sql = sql.where(user_table.columns[key] == value)

        execute(sql, engine)

    def _select_dict(self: object, schema: str, table: str, conditions: dict[str, str]) -> DataFrame:
        from sqlalchemy import MetaData, Table, select
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import read_sql

        engine = create_engine('presto://%s:%d/postgresql/%s' %
                               (PostgresqlConnector._presto['host'],
                                PostgresqlConnector._presto['port'],
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

        engine = create_engine('presto://%s:%d/postgresql/%s' %
                               (PostgresqlConnector._presto['host'],
                                PostgresqlConnector._presto['port'],
                                schema))

        return read_sql(sql, engine)
