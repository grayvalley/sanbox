import configparser


class ConfigReader:

    def __init__(self, file):
        self._config = configparser.ConfigParser()
        self._config.read(file)

    @property
    def simulate(self):
        value = self._config['book']['simulate']
        if value == 'true':
            return True
        elif value == 'false':
            return False
        else:
            raise ValueError('Simulate flag can only be true or false.')

    @property
    def initial_best_bid(self):
        return int(self._config['book']['initial-best-bid'])

    @property
    def initial_best_ask(self):
        return int(self._config['book']['initial-best-ask'])

    @property
    def initial_book_levels(self):
        return int(self._config['book']['initial-levels'])

    @property
    def initial_orders(self):
        return int(self._config['book']['initial-orders'])

    @property
    def initial_order_volume(self):
        return int(self._config['book']['initial-order-volume'])

    @property
    def market_data_address(self):
        return self._config['market-data']['request-address']

    @property
    def market_data_port(self):
        return int(self._config['market-data']['request-port'])

    @property
    def market_report_address(self):
        return self._config['market-report']['request-address']

    @property
    def market_report_port(self):
        return int(self._config['market-report']['request-port'])

    @property
    def order_entry_address(self):
        return self._config['order-entry']['request-address']

    @property
    def order_entry_port(self):
        return int(self._config['order-entry']['request-port'])

    @property
    def display(self):
        return self._config['display']['style']