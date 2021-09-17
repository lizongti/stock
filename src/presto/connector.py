from abc import abstractmethod
from pandas import DataFrame


class Connector(object):
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

    def delete(self: object, schema: str, table: str, data: object = {}):
        if isinstance(data, list):
            self._delete_list(schema, table, data)
        elif isinstance(data, dict):
            self._delete_dict(schema, table, data)
        else:
            self._default(schema, table, data)

    def select(self: object, schema: str, table: str, data: object = {}) -> DataFrame:
        if isinstance(data, list):
            self._select_list(schema, table, data)
        elif isinstance(data, dict):
            self._select_dict(schema, table, data)
        else:
            self._default(schema, table, data)
