class DataSource(object):
    def __init__(self: object, catalog: str, schema: str, table: str):
        self.catalog = catalog
        self.schema = schema
        self.table = table

    def __str__(self: object) -> str:
        return '%s.%s.%s' % (self.catalog,
                             self.schema,
                             self.table)
