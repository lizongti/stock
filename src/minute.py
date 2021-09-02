import akshare as ak
import presto
import stocks
from retrying import retry

schema = "stock"
table = "minute"
keys = ["time", "opening", "closing", "higheast",
        "loweast",  "volume", "turnover", "lastest"]


def update():
    importer = presto.TableRowImporter(schema, table)
    symbols = stocks.get_symbols()
    length = len(symbols)
    for i in range(length):
        symbol = symbols[i]
        print("Symbol[%s](%d/%d): minute is updating.." %
              (symbol, i+1, length), end='')
        __update_symbol(importer, symbol)
        print(" -> Done!")


@retry
def __update_symbol(importer: presto.TableRowImporter, symbol: str):
    print(".", end='')
    df = ak.stock_zh_a_hist_min_em(symbol=symbol)
    for row in df.iterrows():
        values = row[1].values
        minute = values[0]
        key = "%s:%s" % (symbol, minute.replace(
            " ", ":").replace("-", ":"))
        values[0] += ":00.000"
        importer.save(key, values)


if __name__ == '__main__':
    update()
