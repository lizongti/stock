import akshare as ak
import presto

schema = "stock"
table = "minute"
keys = ["time", "opening", "closing", "higheast",
        "loweast",  "volume", "turnover", "lastest"]


def update_one_symbol(importer, symbol):
    df = ak.stock_zh_a_hist_min_em(symbol=symbol)

    for row in df.iterrows():
        values = row[1].values
        minute = values[0]
        key = "%s:%s" % (symbol, minute.replace(" ", ":").replace("-", ":"))
        values[0] += ":00.000"
        importer.save(key, values)


def main():
    importer = presto.TableRowImporter(schema, table)
    update_one_symbol(importer, "601456")


if __name__ == '__main__':
    main()
