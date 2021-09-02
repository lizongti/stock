import akshare as ak
import presto

schema = "stock"
table = "stocks"


def update():
    importer = presto.TableRowImporter(schema, table)

    print("Stocks is updating...", end='')

    df = ak.stock_info_a_code_name()

    for row in df.iterrows():
        values = row[1].values
        key = values[0]
        importer.save(key, values)

    print("Done!")


def get_symbols():
    symbols = []
    df = ak.stock_info_a_code_name()
    for row in df.iterrows():
        values = row[1].values
        key = values[0]
        symbols.append(key)
    return symbols


if __name__ == '__main__':
    update()
