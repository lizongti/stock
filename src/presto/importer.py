import platform
import redis
from pandas import DataFrame
from abc import abstractmethod


class DataSet(object):
    def __init__(self: object, catalog: str, schema: str, table: str):
        self.catalog = catalog
        self.schema = schema
        self.table = table

    def __str__(self: object) -> str:
        return '%s.%s.%s' % (self.catalog,
                             self.schema,
                             self.table)


class Importer(object):
    def __init__(self: object, catalog: str):
        self.catalog = catalog

    def _default(self: object, schema: str, table: str, data: object):
        print('data type is not supported')

    @abstractmethod
    def _insert_df(self: object, schema: str, table: str, df: DataFrame):
        pass

    @abstractmethod
    def _delete_dict(self: object, schema: str, table: str, df: DataFrame):
        pass

    def insert(self: object, schema: str, table: str, data: object):
        if isinstance(data, DataFrame):
            self._insert_df(schema, table, data)
        else:
            self._default(schema, table, data)

    def delete(self: object, schema: str, table: str, data: object):
        if isinstance(data, list):
            self._delete_list(schema, table, data)
        elif isinstance(data, dict):
            self._delete_dict(schema, table, data)
        else:
            self._default(schema, table, data)


class ImporterFactory(object):
    def produce(self: object, case: str):
        return getattr(self, '_'+case, self._default)()

    def _default(cls: object) -> Importer:
        return None

    def _redis(cls: object) -> Importer:
        return RedisImporter()

    def _hive(cls: object) -> Importer:
        return HiveImporter()


class RedisImporter(Importer):
    _catalog = 'redis'
    if(platform.system() == 'Windows'):
        _host = '127.0.0.1'
    else:
        _host = 'pika'
    _port = 9221
    _password = 'adminpass'
    _db = 0

    def __init__(self: object):
        super(RedisImporter, self).__init__(RedisImporter._catalog)
        self.conn = redis.Redis(
            host=RedisImporter._host,
            port=RedisImporter._port,
            password=RedisImporter._password,
            db=RedisImporter._db)

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


class HiveImporter(Importer):
    _catalog = 'hive'
    if(platform.system() == 'Windows'):
        _host = '127.0.0.1'
    else:
        _host = 'presto'
    _port = 8080

    def __init__(self: object):
        super(HiveImporter, self).__init__(HiveImporter._catalog)

    def __del__(self: object):
        pass

    def _insert_df(self: object, schema: str, table: str, df: DataFrame):
        from sqlalchemy.engine import create_engine

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveImporter._host, HiveImporter._port, schema))

        df.to_sql(name=table, con=engine, if_exists='append',
                  index=False, index_label=None, chunksize=None,
                  dtype=None, method='multi')

    def _delete_list(self: object, schema: str, table: str, conditions: list[str]):
        from sqlalchemy import MetaData, Table, delete
        from sqlalchemy.sql.expression import text
        from sqlalchemy.engine import create_engine

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveImporter._host, HiveImporter._port, schema))
        metadata = MetaData(bind=engine)
        user_table = Table(
            table, metadata, autoload=True, autoload_with=engine)

        sql = delete(user_table)
        for condition in conditions:
            sql = sql.where(text(condition))

        conn = engine.connect()
        conn.execute(sql)
        conn.close()

    def _delete_dict(self: object, schema: str, table: str, conditions: dict[str, str]):
        from sqlalchemy import MetaData, Table, delete
        from sqlalchemy.engine import create_engine

        engine = create_engine(
            'presto://%s:%d/hive/%s' %
            (HiveImporter._host, HiveImporter._port, schema))
        metadata = MetaData(bind=engine)
        user_table = Table(
            table, metadata, autoload=True, autoload_with=engine)

        sql = delete(user_table)
        for key, value in conditions.items():
            sql = sql.where(user_table.columns[key] == value)

        conn = engine.connect()
        conn.execute(sql)
        conn.close()


def insert(dataset: DataSet, data: object):
    ImporterFactory().produce(dataset.catalog).insert(
        dataset.schema, dataset.table, data)


def delete(dataset: DataSet, data: object):
    ImporterFactory().produce(dataset.catalog).delete(
        dataset.schema, dataset.table, data)
