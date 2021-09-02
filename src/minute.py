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
        print("Symbol[%s](%d/%d): minute is updating.."
              %(symbol, i+1, length), end='')
        __update_symbol(importer, symbol)
        print(" -> Done!")


@retry(stop_max_attempt_number=100)
def __update_symbol(importer: presto.TableRowImporter, symbol: str):
    print(".", end='')
    df = ak.stock_zh_a_hist_min_em(symbol=symbol)
    for row in df.iterrows():
        minute = row[1].values[0]
        key = "%s:%s" % (symbol, minute.replace(
            " ", ":").replace("-", ":"))
        mintute_parts = minute.split(" ")
        date = mintute_parts[0]
        time = mintute_parts[1]+":00.000"

        values = [symbol, date, time]
        values.extend(row[1].values[1:])
        importer.save(key, values)


if __name__ == '__main__':
    update()
