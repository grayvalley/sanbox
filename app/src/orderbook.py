import sys
import math
from datetime import datetime
from collections import deque # a faster insert/pop queue
from six.moves import cStringIO as StringIO

from decimal import (
    Decimal
)

from .ordertree import (
    OrderTree
)

from .side import (
    Side
)

from .transaction import (
    PassiveParty,
    AggressingParty,
    Transaction,
    TransactionList
)


def get_opposite_side(side):
    if side == Side.B:
        return Side.S
    elif side == Side.S:
        return Side.B
    else:
        raise ValueError('Side has to be either Side.B or Side.S.')


class OrderBook(object):

    def __init__(self, tick_size=0.0001):
        self.tape = deque(maxlen=None)  # Index[0] is most recent trade
        self.bids = OrderTree()
        self.asks = OrderTree()
        self.last_tick = None
        self.last_timestamp = 0
        self.tick_size = tick_size
        self.time = 0
        self.next_order_id = 0

    def update_time(self):
        self.time = datetime.now()

    def increment_next_order_id(self):
        self.next_order_id += 1

    def process_order(self, order, from_data, verbose):

        order['order_id'] = self.next_order_id
        self.increment_next_order_id()

        order_type = order['order_type']

        self.update_time()
        order['timestamp'] = self.time

        if order_type == 'MKT':
            trades = self.process_market_order(order, verbose)

        elif order_type == 'LMT':
            order['price'] = Decimal(order['price'])
            trades, order_in_book = self.process_limit_order(order, from_data, verbose)
        else:
            sys.exit("order_type for process_order() is neither 'market' or 'limit'")

        return trades, order

    def process_limit_order(self, order, from_data, verbose):

        trades = TransactionList()

        quantity_to_trade = order['quantity']
        side = order['side']
        price = order['price']

        if side == Side.B:

            while self.asks and price >= self.asks.min_price() and quantity_to_trade > 0:

                best_price_asks = self.asks.min_price_list()
                quantity_to_trade, new_trades = self.process_order_list(
                    side, best_price_asks, quantity_to_trade, order, verbose)

                if new_trades is not None:
                    trades.add_transactions(new_trades)

            # Update order quantity
            order['quantity'] = quantity_to_trade

            # If volume remains, need to update the book with new quantity
            if quantity_to_trade > 0:
                self.bids.insert_order(order)

        elif side == Side.S:

            while self.bids and price <= self.bids.max_price() and quantity_to_trade > 0:

                best_price_bids = self.bids.max_price_list()
                quantity_to_trade, new_trades = self.process_order_list(
                    side, best_price_bids, quantity_to_trade, order, verbose)

                if new_trades is not None:
                    trades.add_transactions(new_trades)

            # Update order quantity
            order['quantity'] = quantity_to_trade

            # If volume remains, need to update the book with new quantity
            if quantity_to_trade > 0:
                self.asks.insert_order(order)

        else:
            sys.exit('process_limit_order() given neither "bid" nor "ask"')

        return trades, order

    def process_order_list(self, side, order_list, quantity_still_to_trade, quote, verbose):
        '''
        Takes an OrderList (stack of orders at one price) and an incoming order and matches
        appropriate trades given the order's quantity.
        '''
        trades = []
        trade_list = TransactionList()
        quantity_to_trade = quantity_still_to_trade

        # Match trades
        while len(order_list) > 0 and quantity_to_trade > 0:

            # Price time priority
            head_order = order_list.get_head_order()
            traded_price = head_order.price
            counter_party = head_order.order_id
            new_book_quantity = None
            if quantity_to_trade < head_order.quantity:
                traded_quantity = quantity_to_trade
                # Do the transaction
                new_book_quantity = head_order.quantity - quantity_to_trade
                head_order.update_quantity(new_book_quantity, head_order.timestamp)
                quantity_to_trade = 0
            elif quantity_to_trade == head_order.quantity:
                traded_quantity = quantity_to_trade
                if side == Side.B:
                    self.asks.remove_order_by_id(head_order.order_id)
                else:
                    self.bids.remove_order_by_id(head_order.order_id)

                quantity_to_trade = 0
            else:  # quantity to trade is larger than the head order
                traded_quantity = head_order.quantity
                if side == Side.B:
                    self.asks.remove_order_by_id(head_order.order_id)
                else:
                    self.bids.remove_order_by_id(head_order.order_id)
                quantity_to_trade -= traded_quantity

            '''
            Aggressing party:
            
            The side of the transaction is being determined by the original side
            of the aggressing party.
            '''
            aggressor = AggressingParty()
            aggressor.id = quote['order_id']
            aggressor.side = side
            aggressor.order_type = quote['order_type']
            '''
            Passive party:
            '''
            passive = PassiveParty()
            passive.id = counter_party
            passive.order_id = head_order.order_id
            passive.quantity_remaining = new_book_quantity
            passive.side = get_opposite_side(side)

            transaction = Transaction()
            transaction.aggressor = aggressor
            transaction.passive = passive
            transaction.timestamp = self.time
            transaction.traded_price = traded_price
            transaction.traded_quantity = traded_quantity

            trades.append(transaction)

        trade_list.add_transactions(trades)

        return quantity_to_trade, trade_list

    def process_market_order(self, quote, verbose):

        trades = TransactionList()
        quantity_to_trade = quote['quantity']
        side = quote['side']

        # Match trades using price time priority rule
        if side == Side.B:
            while quantity_to_trade > 0 and self.asks:
                best_price_asks = self.asks.min_price_list()
                quantity_to_trade, new_trades = self.process_order_list(
                    Side.B, best_price_asks, quantity_to_trade, quote, verbose)
                if new_trades is not None:
                    trades.add_transactions(new_trades)

        elif side == Side.S:
            while quantity_to_trade > 0 and self.bids:
                best_price_bids = self.bids.max_price_list()
                quantity_to_trade, new_trades = self.process_order_list(
                    Side.S, best_price_bids, quantity_to_trade, quote, verbose)
                if new_trades is not None:
                    trades.add_transactions(new_trades)
        else:
            sys.exit('process_market_order() recieved neither "bid" nor "ask"')

        return trades

    def get_order(self, order_id):

        if self.bids.order_exists(order_id):
            return self.bids.get_order(order_id)
        elif self.asks.order_exists(order_id):
            return self.asks.get_order(order_id)
        else:
            return None

    def cancel_order(self, side, order_id, time=None):
        if time:
            self.time = time
        else:
            self.update_time()
        if side == Side.B:
            if self.bids.order_exists(order_id):
                self.bids.remove_order_by_id(order_id)
        elif side == Side.S:
            if self.asks.order_exists(order_id):
                self.asks.remove_order_by_id(order_id)
        else:
            sys.exit('cancel_order() given neither "bid" nor "ask"')

    def modify_order(self, order_id, order_update, time=None):
        if time:
            self.time = time
        else:
            self.update_time()
        side = order_update['side']
        order_update['order_id'] = order_id
        order_update['timestamp'] = self.time

        if side == Side.B:
            if self.bids.order_exists(order_update['order_id']):
                self.bids.update_order(order_update)

        elif side == Side.S:
            if self.asks.order_exists(order_update['order_id']):
                self.asks.update_order(order_update)

        else:
            sys.exit('modify_order() given neither "bid" nor "ask"')

    def get_volume_at_price(self, side, price):

        price = Decimal(price)

        if side == Side.B:
            volume = 0
            if self.bids.price_exists(price):
                volume = self.bids.get_price_list(price).volume
            return volume
        elif side == Side.S:
            volume = 0
            if self.asks.price_exists(price):
                volume = self.asks.get_price_list(price).volume
            return volume
        else:
            sys.exit('get_volume_at_price() given neither "bid" nor "ask"')

    def get_best_bid(self):
        return self.bids.max_price()

    def get_worst_bid(self):
        return self.bids.min_price()

    def get_best_ask(self):
        return self.asks.min_price()

    def get_worst_ask(self):
        return self.asks.max_price()

    def print(self):

        tempfile = StringIO()

        tempfile.write("--- [Asks] ---\n")
        if self.asks != None and len(self.asks) > 0:
            for price, order_list in reversed(self.asks.price_map.items()):
                tempfile.write(f'{price} - {order_list.volume}\n')

        tempfile.write("\n")
        tempfile.write("--- [Bids] ---\n")
        if self.bids != None and len(self.bids) > 0:
            for price, order_list in reversed(self.bids.price_map.items()):
                tempfile.write(f'{price} - {order_list.volume}\n')

        tempfile.write("\n")
        print(tempfile.getvalue())

    def __str__(self):
        tempfile = StringIO()
        tempfile.write("***Bids***\n")
        if self.bids != None and len(self.bids) > 0:
            for key, value in reversed(self.bids.price_map.items()):
                tempfile.write('%s' % value)
        tempfile.write("\n***Asks***\n")
        if self.asks != None and len(self.asks) > 0:
            for key, value in self.asks.price_map.items():
                tempfile.write('%s' % value)

        if self.tape != None and len(self.tape) > 0:
            tempfile.write("\n***Trades***\n")
            num = 0
            for entry in self.tape:
                if num < 10: # get last 5 entries
                    tempfile.write(str(entry['quantity']) + " @ " + str(entry['price']) + " (" + str(entry['timestamp']) + ") " + str(entry['party1'][0]) + "/" + str(entry['party2'][0]) + "\n")
                    num += 1
                else:
                    break
        tempfile.write("\n")
        return tempfile.getvalue()

