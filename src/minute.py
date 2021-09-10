import akshare as ak
import stocks
from retrying import retry

schema = "stock"
table = "minute"
keys = ["time", "opening", "closing", "higheast",
        "loweast",  "volume", "turnover", "lastest"]


def update():
    __update_symbols_minutes(
        RedisImporter(schema, table),
        TxtImporter(schema, table),
    )


def __update_symbols_minutes(importers: list[object]):
    symbols = stocks.get_symbols()
    length = len(symbols)
    for i in range(length):
        symbol = symbols[i]
        print("Symbol[%s](%d/%d): minute is updating.."
              % (symbol, i+1, length), end='')
        __update_symbol_minutes(importers, symbol)
        print(" -> Done!")


@retry(stop_max_attempt_number=100)
def __update_symbol_minutes(importers: list[object], symbol: str):
    print(".", end='')

    df = ak.stock_zh_a_hist_min_em(symbol=symbol)
    pairs = []
    for row in df.iterrows():
        minute = row[1].values[0]
        key = "%s:%s" % (symbol, minute.replace(
            " ", ":").replace("-", ":"))
        mintute_parts = minute.split(" ")
        date = mintute_parts[0]
        time = mintute_parts[1]+":00.000"

        values = [symbol, date, time]
        values.extend(row[1].values[1:])
        pairs.append((key, values))
    for importer in importers:
        importer.save(pairs)


# def get_symbols() -> list[str]:
#     symbols = []
#     df = ak.stock_info_a_code_name()
#     for row in df.iterrows():
#         values = row[1].values
#         key = values[0]
#         symbols.append(key)
#     return symbols


if __name__ == '__main__':
    update()
