
from . import DataSource
from .connector_factory import ConnectorFactory


_connect_factory = ConnectorFactory()


def insert(data_source: DataSource, data: object):
    _connect_factory.produce(data_source.catalog).insert(
        data_source.schema, data_source.table, data)


def delete(data_source: DataSource, data: object):
    _connect_factory.produce(data_source.catalog).delete(
        data_source.schema, data_source.table, data)
