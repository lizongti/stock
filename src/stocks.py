import akshare as ak
import presto
from retrying import retry

schema = "stock"
table = "stocks"


def update():
    importer = presto.RedisRowsImporter(schema, table)
    print("Stocks is updating..", end='')
    __update_stocks(importer)
    print(" -> Done!")


@retry(stop_max_attempt_number=100)
def __update_stocks(importer: presto.RedisRowsImporter):
    print(".", end='')
    df = ak.stock_info_a_code_name()
    rows = []
    for row in df.iterrows():
        values = row[1].values
        key = values[0]
        rows.append((key, values))
    importer.save(rows)


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
