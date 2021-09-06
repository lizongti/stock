import time
from tools.file_io import write_txt


class TxtImporter:
    def __init__(self: object, schema: str, table: str):
        date = time.strftime("%Y-%m-%d", time.localtime(time.time()))
        self.name = ".".join([schema, table, date])
        pass

    def save(self: object, pairs: list[tuple]):
        write_txt(self.name, pairs)
