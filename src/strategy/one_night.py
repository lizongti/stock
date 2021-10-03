if __name__ == '__main__':
    import sys
    from os.path import dirname, abspath
    sys.path.append(dirname(dirname(abspath(__file__))))

from retrying import retry
from tools import time


class OneNightStrategy:
    def run(self: object):

        # def select_quote_change():
        #     pass

        # def select_quote_change():