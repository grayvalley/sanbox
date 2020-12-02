import threading
from queue import Queue
from copy import deepcopy
from .orderbook import OrderBook


class GlobalState:

    def __init__(self, config):
        self._cfg = config
        self._lob = OrderBook()
        self._event_queue = Queue()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        self._order_clients = {}

        self._market_data_clients = []

        self._simulation_threads = []
        self._order_client_threads = []
        self._market_data_client_threads = []

        self._threads = []

    @property
    def stopper(self):
        return self._stop_event

    @property
    def config(self):
        return self._cfg

    @property
    def lock(self):
        return self._lock

    @property
    def event_queue(self):
        return self._event_queue

    def get_market_data_clients(self):
        return self._market_data_clients

    def add_simulation_thread(self, thread):
        self._simulation_threads.append(thread)

    def get_simulation_threads(self):
        return self._simulation_threads

    def add_order_client_thread(self, thread):
        self._order_client_threads.append(thread)

    def get_order_client_threads(self):
        return self._order_client_threads

    def add_market_data_client_thread(self, thread):
        self._market_data_client_threads.append(thread)

    def get_market_data_client_threads(self):
        return self._market_data_client_threads

    def get_threads(self):
        return self._threads

    def add_order_client(self, trader_id, client):

        self._lock.acquire()
        self._order_clients.update({trader_id: client})
        self._lock.release()

    def remove_order_client(self, trader_id):

        self._lock.acquire()
        del self._order_clients[trader_id]
        self._lock.release()

    def get_order_client_nts(self, trader_id):
        if trader_id in self._order_clients:
            client = self._order_clients[trader_id]
            return client
        else:
            raise RuntimeError(f"TraderId: {trader_id} not found from OrderClients!")

    def add_market_data_client(self, client):

        #with self._lock:
            self._market_data_clients.append(client)

    def remove_market_data_client(self, client):

        with self._lock:
            self._market_data_clients.remove(client)

    def get_current_lob_state(self):
        """
        Returns a reference to the current LOB.
        :return:
        """

        return self._lob

    def add_to_event_queue(self, event):

        with self._lock:
            self._event_queue.put(event)

