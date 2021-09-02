import akshare as ak
import presto
import stocks

schema = "stock"
table = "minute"
keys = ["time", "opening", "closing", "higheast",
        "loweast",  "volume", "turnover", "lastest"]


def update():
    importer = presto.TableRowImporter(schema, table)
    symbols = stocks.get_symbols()
    for symbol in symbols:
        print("Symbol[%s]: minute is updating..." % (symbol), end='')

        df = ak.stock_zh_a_hist_min_em(symbol=symbol)

        for row in df.iterrows():
            values = row[1].values
            minute = values[0]
            key = "%s:%s" % (symbol, minute.replace(
                " ", ":").replace("-", ":"))
            values[0] += ":00.000"
            importer.save(key, values)

        print("Done!")


if __name__ == '__main__':
    update()
