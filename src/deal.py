import akshare as ak
import presto
import stocks
from retrying import retry

schema = "stock"
table = "deal"


def update():
    importer = presto.TableRowImporter(schema, table)
    symbols = stocks.get_symbols()
    length = len(symbols)
    for i in range(length):
        symbol = symbols[i]
        print("Symbol[%s](%d/%d): deal is updating.." %
              (symbol, i+1, length), end='')
        __update_symbol_day(importer, symbol, date)
        print(" -> Done!")

    print("")


def __update_symbol_day(importer: presto.TableRowImporter,
                        symbol: str, date: str):
    stock_zh_a_tick_tx_df = ak.stock_zh_a_tick_tx(
    code="sh600848", trade_date="20191011")
print(stock_zh_a_tick_tx_df)
