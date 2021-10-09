if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

import presto
from pandas.core.frame import DataFrame
from retrying import retry
from tools import time


class OneNightStrategy(presto.DataSource):
    _catalog = 'postgresql'
    _schema = 'strategy'
    _table = 'one_night'

    def run(self: object):

    def _select(self: object) -> DataFrame:
        sql = """
            set session query_max_stage_count = 1000;
            select 
        """
