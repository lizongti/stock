import akshare as ak
from presto.redis import RedisImporter
from presto.hive import TxtImporter
from retrying import retry

schema = "stock"
table = "stocks"


def update():
    print("Stocks is updating..", end='')
    __update_stocks([
        RedisImporter(schema, table),
        TxtImporter(schema, table),
    ])
    print(" -> Done!")


@retry(stop_max_attempt_number=100)
def __update_stocks(importers: list[object]):
    print(".", end='')
    df = ak.stock_info_a_code_name()
    pairs = []
    for row in df.iterrows():
        values = row[1].values
        key = values[0]
        pairs.append((key, values))
    for importer in importers:
        importer.save(pairs)


def get_symbols() -> list[str]:
    symbols = []
    df = ak.stock_info_a_code_name()
    for row in df.iterrows():
        values = row[1].values
        key = values[0]
        symbols.append(key)
    return symbols


if __name__ == '__main__':
    update()
