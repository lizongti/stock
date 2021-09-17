
from .connector import Connector
from .redis_connector import RedisConnector
from .hive_connector import HiveConnector
from .postgresql_connector import PostgresqlConnector


class ConnectorFactory(object):
    def produce(self: object, case: str):
        return getattr(self, '_'+case, self._default)()

    def _default(cls: object) -> Connector:
        return None

    def _redis(cls: object) -> Connector:
        return RedisConnector()

    def _hive(cls: object) -> Connector:
        return HiveConnector()

    def _postgresql(cls: object) -> Connector:
        return PostgresqlConnector()
