import numpy as np
import random
import time
from abc import ABCMeta, abstractmethod
import json
from decimal import Decimal
from .side import (
    Side,
    side_to_str
)
from .event import (
    EventTypes,
    Add,
    Cancel,
    MarketOrder
)
from src.transaction import (
    TransactionList
)

import src.messaging as messaging


class EventGenerator:

    def __init__(self, thread_id, event_type, event_side, level, arrival_rate, tick_size):
        self._thread_id = thread_id
        self._event_type = event_type
        self._event_side = event_side
        self._level = level
        self._arrival_rate = arrival_rate
        self._tick_size = tick_size
        self._rng = np.random.RandomState()
        self._rng.seed(np.random.randint(1, 100))

    @property
    def thread_id(self):
        return self._thread_id

    @property
    def arrival_rate(self):
        return self._arrival_rate

    @property
    def type(self):
        return self._event_type

    @property
    def side(self):
        return self._event_side

    @property
    def level(self):
        return self._level

    @property
    def tick_size(self):
        return self._tick_size

    def _infer_price_level(self, state):
        """
        Infers correct price level based on the reference level.
        """

        lob = state.get_current_lob_state()

        # Calculate peg, i.e. the offset w.r.t reference price
        peg = self.level * self.tick_size

        # Get current best bid and ask
        best_bid = lob.get_best_bid()
        best_ask = lob.get_best_ask()

        # Calculate price level
        if self.side == Side.B:
            if best_ask is None:  # in case ask side is empty --> peg against the best bid
                price_level = best_bid - peg
            else:
                price_level = best_ask - peg
        elif self.side == Side.S:
            if best_bid is None:  # in case ask side is empty --> peg against the best ask
                price_level = best_ask + peg
            else:
                price_level = best_bid + peg

        return price_level

    def _generate_random_limit_order_quantity(self, price):
        """
        Generates random order quantity
        """

        quantity = self._rng.randint(1, 10)
        #quantity = max(self._rng.geometric(0.25, 1)[0], 1)

        return Decimal(quantity)

    def _generate_random_market_order_quantity(self, price, state):
        """
        Generates random order quantity for a market order.
        TODO: get distribution parameters from configuration
        """

        lob = state.get_current_lob_state()

        if self.side == Side.B:  # buy market order

            quantity = max(self._rng.geometric(0.05, 1)[0].item(), 1)

        elif self.side == Side.S:  # sell market order

            quantity = max(self._rng.geometric(0.05, 1)[0].item(), 1)

        return Decimal(quantity)

    def _choose_random_order_id(self, price, state):
        """
        Chooses a random order from the LOB for a given
        price level and side.

        Note:
        If order id cannot be generated function returns None.

        """
        lob = state.get_current_lob_state()
        if self.side == Side.B:
            if lob.bids.price_exists(price):
                order_list = lob.bids.get_price_list(price)
                if order_list.volume > 0:
                    order_ids = [order.order_id for order in list(order_list)]
                    chosen = random.choice(order_ids)
                    return chosen
                else:
                    # We cannot cancel if the price level is empty
                    return None
            else:
                # We cannot cancel if the price level is empty
                return None

        elif self.side == Side.S:
            if lob.asks.price_exists(price):
                order_list = lob.asks.get_price_list(price)
                if order_list.volume > 0:
                    order_ids = [order.order_id for order in list(order_list)]
                    chosen = random.choice(order_ids)
                    return chosen
                else:
                    # We cannot cancel if the price level is empty
                    return None
            else:
                # We cannot cancel if the price level is empty
                return None

        else:
            raise ValueError("Side not valid.")

    def _create_new_limit_order_addition(self, state):
        """
        Creates a new limit order addition event.

        Format understood by the OrderBook:

        {'type': 'limit',
        'side': 'bid',
        'quantity': 1,
        'price': 97}
        """
        event = Add()
        event.price = self._infer_price_level(state)
        event.quantity = self._generate_random_limit_order_quantity(state)
        event.side = self.side

        return event

    def _create_new_limit_order_cancel(self, state):
        """
        Creates a new limit order addition event.

        Format understood by the OrderBook:
        """
        event = Cancel()
        event.price = self._infer_price_level(state)
        event.order_id = self._choose_random_order_id(event.price, state)
        event.side = self.side

        return event

    def _create_new_market_order(self, state):
        """
        Creates a new market order event.

        Format understood by the OrderBook:

        {'type': 'market',
         'side': 'ask',
         'quantity': 40,
         'trade_id': 111}
        """
        event = MarketOrder()
        event.quantity = self._generate_random_market_order_quantity(state, state)
        event.side = self.side

        return event

    def sleep_until_next_event(self, state):
        """
        Sleeps a random time determined by an exponential random variable.
        """
        numpy_beta = 1.0 / self.arrival_rate
        time_delta_seconds = np.random.exponential(numpy_beta, 1)[0]
        millis_to_sleep = int(time_delta_seconds * 1000)
        n_loops = int(millis_to_sleep / 10)
        for i in range(0, n_loops):
            if state.stopper.is_set():
                break
            time.sleep(0.010)

    def create_event(self, state):
        """
        Creates a new market event.
        """

        if self.type in [EventTypes.ADD]:
            event = self._create_new_limit_order_addition(state)

        elif self.type in [EventTypes.CANCEL]:
            event = self._create_new_limit_order_cancel(state)

        elif self.type in [EventTypes.MARKET_ORDER]:
            event = self._create_new_market_order(state)
        else:
            raise ValueError()

        return event

    def __str__(self):
        s = f'Thread id: {self.thread_id}, event type: {self.type}, side: {self.side}, level: {self.level}'
        return s


def _create_add_message_from_add_event(order):
    """
    Creates add message from an order
    :param order:
    :return:
    """
    message = {}
    message.update({"message-type": "A"})
    message.update({"order-id": order["order_id"]})
    message.update({"price": int(order["price"])})
    message.update({"quantity": int(order["quantity"])})
    message.update({"side": side_to_str(order["side"])})
    message.update({"timestamp": order["timestamp"]})
    message.update({"snapshot": 0})
    return message


def event_generation_loop(state, generator):
    """
    Runs order book events for one event type.
    """
    if not isinstance(generator, EventGenerator):
        raise TypeError("Thread function now takes parameter class.")

    while not state.stopper.is_set():

        # Sleep until next event
        generator.sleep_until_next_event(state)

        state.lock.acquire()

        # Create event
        event = generator.create_event(state)

        if event is not None:

            lob = state.get_current_lob_state()

            if event.event_type in [EventTypes.ADD]:
                _, order_in_book, _ = lob.process_order(event.to_lob_format(), False, False)
                state.event_queue.put(_create_add_message_from_add_event(order_in_book))

            elif event.event_type in [EventTypes.CANCEL]:
                if event.order_id is not None:
                    lob.cancel_order(event.side, event.order_id)
                    state.event_queue.put(event.get_message())

            elif event.event_type in [EventTypes.MARKET_ORDER]:

                transactions, _, _ = lob.process_order(event.to_lob_format(), False, False)

                # Generate trade messages
                aggressor_messages, passive_messages = transactions.get_trade_messages()

                # Send order executed message(s) to the passive side of the transaction.
                # If passive_trader_id is None the order was simulated.
                for passive_trader_id, msg in passive_messages:
                    if passive_trader_id is not None:
                        passive_side_client = state.get_order_client_nts(passive_trader_id)
                        if passive_side_client is not None:
                            messaging.send_data(
                                passive_side_client.socket,
                                json.dumps(msg),
                                passive_side_client.encoding)

                # Publish trade(s) via the public market data feed
                for msg in aggressor_messages:
                    state.event_queue.put(msg.get_message())

                # Publish remove and modify messages via the public market data feed
                remove_and_modify_messages = transactions.get_remove_and_modify_messages()
                for msg in remove_and_modify_messages:
                    state.event_queue.put(msg)

        state.lock.release()

    print('Event generation stopped.')
