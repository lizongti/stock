if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import akshare
import presto
from retrying import retry
from tools import time

import akshare as ak
stock_a_indicator_df = ak.stock_a_lg_indicator()
print(stock_a_indicator_df)

# trade_date	datetime	Y	交易日
# pe	float	Y	市盈率
# pe_ttm	float	Y	市盈率TTM
# pb	float	Y	市净率
# ps	float	Y	市销率
# ps_ttm	float	Y	市销率TTM
# dv_ratio	float	Y	股息率
# dv_ttm	float	Y	股息率TTM
# total_mv	float	Y	总市值
