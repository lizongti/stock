import platform
import redis
from pandas import DataFrame
from abc import abstractmethod


class DataSet(object):
    def __init__(self: object, catalog: str, schema: str, table: str):
        self.catalog = catalog
        self.schema = schema
        self.table = table


class Importer(object):
    def __init__(self: object, catalog: str):
        self.catalog = catalog

    def _load_default(self: object, schema: str, table: str, data: object):
        print("data type cannot be imported")

    @abstractmethod
    def _load_df(self: object, schema: str, table: str, df: DataFrame):
        pass

    def load(self: object, schema: str, table: str, data: object):
        if isinstance(data, DataFrame):
            self._load_df(schema, table, data)
        else:
            self._load_default(schema, table, data)


class ImporterFactory(object):
    def produce(self: object, case: str):
        return getattr(self, "_"+case, self._default)()

    def _default(cls: object) -> Importer:
        return None

    def _redis(cls: object) -> Importer:
        return RedisImporter()

    def _hive(cls: object) -> Importer:
        return None


class RedisImporter(Importer):
    _catalog = "redis"

    def __init__(self: object):
        super(RedisImporter, self).__init__(RedisImporter._catalog)
        self._prepare_redis()

    def _load_df(self: object, schema: str, table: str, df: DataFrame):
        with self.conn.pipeline(transaction=False) as pipe:
            for index in df.index:
                mapping = df.loc[index].to_dict()
                name = ".".join([schema, table, mapping['key']])
                pipe.hmset(name, mapping)
            pipe.execute()

    def _prepare_redis(self: object):
        if(platform.system() == 'Windows'):
            self.host = "127.0.0.1"
            self.port = 9221
        else:
            self.host = "pika"
            self.port = 9221
        self.conn = redis.Redis(host=self.host, port=self.port)


def load(dataset: DataSet, data: object):
    ImporterFactory().produce(dataset.catalog).load(
        dataset.schema, dataset.table, data)
