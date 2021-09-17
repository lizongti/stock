import redis
from pandas import DataFrame
from .connector import Connector


class RedisConnector(Connector):
    _catalog = 'redis'
    _pika = {
        'host': 'pika',
        'port': 9221,
        'password': 'adminpass',
        'db': 0,
    }
    _presto = {
        'host': 'presto',
        'port': 8080,
    }

    def __init__(self: object):
        super(RedisConnector, self).__init__(RedisConnector._catalog)
        self.conn = redis.Redis(
            host=RedisConnector._pika['host'],
            port=RedisConnector._pika['port'],
            password=RedisConnector._pika['password'],
            db=RedisConnector._pika['db'])

    def __del__(self: object):
        self.conn.close()

    def _insert_df(self: object, schema: str, table: str, df: DataFrame):
        with self.conn.pipeline(transaction=False) as pipe:
            for index in df.index:
                mapping = df.loc[index].to_dict()
                name = '.'.join([schema, table, mapping['key']])
                pipe.hmset(name, mapping)
            pipe.execute()

    def _delete_list(self: object, schema: str, table: str, conditions: list[str]):
        # TODO
        pass

    def _delete_dict(self: object, schema: str, table: str, conditions: dict[str, str]):
        # TODO
        pass

    def _select_list(self: object, schema: str, table: str, conditions: list[str]) -> DataFrame:
        from sqlalchemy import MetaData, Table, select
        from sqlalchemy.sql.expression import text
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import read_sql

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (RedisConnector._presto['host'], RedisConnector._presto['port'], schema))
        metadata = MetaData(bind=engine)
        user_table = Table(table, metadata, autoload=True,
                           autoload_with=engine)

        sql = select(user_table)
        for condition in conditions:
            sql = sql.where(text(condition))

        return read_sql(sql, engine)

    def _select_dict(self: object, schema: str, table: str, conditions: dict[str, str]) -> DataFrame:
        from sqlalchemy import MetaData, Table, select
        from sqlalchemy.engine import create_engine
        from pandas.io.sql import read_sql

        engine = create_engine(
            'presto://%s:%d/redis/%s' %
            (RedisConnector._presto['host'], RedisConnector._presto['port'], schema))
        metadata = MetaData(bind=engine)
        user_table = Table(table, metadata, autoload=True,
                           autoload_with=engine)

        sql = select(user_table)
        for key, value in conditions.items():
            sql = sql.where(user_table.columns[key] == value)

        df = read_sql(sql, engine)
        print(df)
        return df
