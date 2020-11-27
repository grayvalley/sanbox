import threading
import logging
import time

from src.state import (
    GlobalState
)

from src.config_reader import (
    ConfigReader
)

from src.side import (
    Side
)

from src.order_entry import (
    accept_new_order_entry_clients
)

from src.market_data import (
    accept_new_market_data_clients,
    public_market_data_feed
)

from src.simulation import (
    run_market_data_simulation
)

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO,
                    datefmt="%H:%M:%S")


def main():

    config = ConfigReader("src/etc/config.ini")

    state = GlobalState(config)

    # Create thread for the public market data feed
    run_public_market_data_feed = threading.Thread(
        target=public_market_data_feed,
        args=(config, state,))
    run_public_market_data_feed.start()
    time.sleep(0.1)

    # Start listening to order entry requests
    order_entry_thread = threading.Thread(
        target=accept_new_order_entry_clients,
        args=(config, state,))
    order_entry_thread.start()
    time.sleep(0.1)

    # Start listening to market data subscriptions
    market_data_thread = threading.Thread(
        target=accept_new_market_data_clients,
        args=(config, state,))
    market_data_thread.start()

    if config.simulate:
        # Initialize order book
        limit_orders = []
        for price in range(config.initial_best_ask, config.initial_best_ask + config.initial_book_levels):
            for i in range(0, config.initial_orders):
                order = {'order_type': 'LMT',
                         'side': Side.S,
                         'quantity': config.initial_order_volume,
                         'price': price}
                limit_orders.append(order)

        for price in range(config.initial_best_bid, config.initial_best_bid - config.initial_book_levels, -1):
            for i in range(0, config.initial_orders):
                order = {'order_type': 'LMT',
                         'side': Side.B,
                         'quantity': config.initial_order_volume,
                         'price': price}
                limit_orders.append(order)

        # Add orders to order book
        lob = state.get_current_lob_state()
        for order in limit_orders:
            trades = lob.process_order(order, False, False)

    # Start producing market data events
    if config.simulate:
        run_market_data_simulation(config, state)

    try:
        while 1:
            time.sleep(.1)
    except KeyboardInterrupt:
        print("Attempting to close threads")
        state.stopper.set()
        for thread in state.get_simulation_threads():
            thread.join()
        order_entry_thread.join()
        market_data_thread.join()
        print("Threads successfully closed")

    print("System shutdown.")


if __name__ == "__main__":
    main()
