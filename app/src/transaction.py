from decimal import Decimal
import uuid

from .side import (
    Side,
    side_to_str
)


class PassiveParty:

    def __init__(self):

        self._id = None
        self._trader_id = None
        self._side = None
        self._order_id = None
        self._quantity_remaining = None

    @property
    def trader_id(self):
        return self._trader_id

    @trader_id.setter
    def trader_id(self, value):
        if not isinstance(value, (uuid.UUID, type(None))):
            raise TypeError('TraderId has to be type of <uuid.UUID>.')
        self._trader_id = value

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        if not isinstance(value, int):
            raise TypeError('Id has to be type of <int>.')
        self._id = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        if value not in [Side.B, Side.S]:
            raise TypeError('Side has to be either Side.BID or Side.ASK.')
        self._side = value

    @property
    def order_id(self):
        return self._order_id

    @order_id.setter
    def order_id(self, value):
        if not isinstance(value, int):
            raise TypeError('Order_id has to be type of <int>.')
        self._order_id = value

    @property
    def quantity_remaining(self):
        return self._quantity_remaining

    @quantity_remaining.setter
    def quantity_remaining(self, value):
        if not isinstance(value, (Decimal, type(None))):
            raise TypeError(f'Quantity remaining has to be type of <int>, was {type(value)}.')
        if value is None:
            self._quantity_remaining = 0
        else:
            self._quantity_remaining = int(value)


class AggressingParty:

    def __init__(self):
        self._id = None
        self._trader_id = None
        self._side = None
        self._order_type = None

    @property
    def trader_id(self):
        return self._trader_id

    @trader_id.setter
    def trader_id(self, value):
        if not isinstance(value, (uuid.UUID, type(None))):
            raise TypeError('TraderId has to be type of <uuid.UUID>.')
        self._trader_id = value

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        if not isinstance(value, int):
            raise TypeError('Id has to be type of <int>.')
        self._id = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        if value not in [Side.B, Side.S]:
            raise TypeError('Side has to be either Side.BID or Side.ASK.')
        self._side = value

    @property
    def order_type(self):
        return self._order_type

    @order_type.setter
    def order_type(self, value):
        self._order_type = value


class Transaction:

    def __init__(self):

        self._aggressor = None
        self._passive = None
        self._timestamp = None
        self._traded_price = None
        self._traded_quantity = None

    @property
    def aggressor(self):
        return self._aggressor

    @aggressor.setter
    def aggressor(self, value):
        if not isinstance(value, AggressingParty):
            raise TypeError('Aggressor has to be type of <AggressingParty>.')
        self._aggressor = value

    @property
    def passive(self):
        return self._passive

    @passive.setter
    def passive(self, value):
        if not isinstance(value, PassiveParty):
            raise TypeError('Passive has to be type of <PassiveParty>.')
        self._passive = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        #if not isinstance(value, (int, datetime, str)):
        #    raise TypeError(f'Timestamp has to be <int> or <datetime> or <str>, was {type(value)}.')
        self._timestamp = value

    @property
    def traded_price(self):
        return self._traded_price

    @traded_price.setter
    def traded_price(self, value):
        if not isinstance(value, Decimal):
            raise TypeError('Traded price has to be <Decimal>.')
        self._traded_price = value

    @property
    def traded_quantity(self):
        return self._traded_quantity

    @traded_quantity.setter
    def traded_quantity(self, value):
        """
        Traded quantity is Decimal when
        """
        if not isinstance(value, Decimal):
            raise TypeError(f'Traded quantity was {type(value)}.')
        self._traded_quantity = int(value)


# TODO: make property setters and getters with proper error handling
class SelfMatchCancel:

    def __init__(self):
        self.order_id = None
        self.side = None
        self.quantity = None
        self.price = None
        self.timestamp = None
        self.trader_id = None
        self.instrument = None


class TransactionList:

    def __init__(self):

        self._trade_list = []

    def is_empty(self):

        if len(self._trade_list) == 0:
            return True
        else:
            return False

    def add_transactions(self, trades):

        if isinstance(trades, TransactionList):
            self._trade_list += trades._trade_list

        elif isinstance(trades, list):
            self._trade_list += trades

    def get_trade_messages(self):
        """

        """
        aggressor_messages = []
        for transaction in self._trade_list:

            message = {}
            message.update({'message-type': 'E'})
            message.update({'order-type': transaction.aggressor.order_type})
            message.update({'timestamp': str(transaction.timestamp)})
            message.update({'price': int(transaction.traded_price)})
            message.update({'order-id': transaction.aggressor.id})
            message.update({'quantity': transaction.traded_quantity})
            if transaction.aggressor.side == Side.B:
                message.update({'side': 'B'})
            elif transaction.aggressor.side == Side.S:
                message.update({'side': 'S'})
            else:
                raise ValueError()
            aggressor_messages.append(message)

        passive_messages = []
        for transaction in self._trade_list:
            message.update({'message-type': 'E'})
            message.update({'order-type': "LMT"})
            message.update({'timestamp': str(transaction.timestamp)})
            message.update({'price': int(transaction.traded_price)})
            message.update({'order-id': transaction.passive.id})
            message.update({'quantity': transaction.traded_quantity})
            # TODO: replace with the side_to_str function
            if transaction.passive.side == Side.B:
                message.update({'side': 'B'})
            elif transaction.passive.side == Side.S:
                message.update({'side': 'S'})
            else:
                raise ValueError()
            passive_messages.append((transaction.passive.trader_id, message))

        return aggressor_messages, passive_messages

    def get_remove_and_modify_messages(self):
        """
        Creates one JSON message for each individual passive order
        found in the trade list.
        """

        messages = []
        for transaction in self._trade_list:

            message = {}
            message.update({'timestamp': str(transaction.timestamp)})
            message.update({'side': side_to_str(transaction.passive.side)})
            message.update({'price': int(transaction.traded_price)})
            message.update({'order-id': transaction.passive.id})

            # Remove
            if transaction.passive.quantity_remaining == 0:
                message.update({'message-type': 'X'})

            # Modify
            elif transaction.passive.quantity_remaining > 0:
                message.update({'message-type': 'M'})
                message.update({'quantity': int(transaction.passive.quantity_remaining)})

            else:
                raise ValueError(f'Quantity remaining invalid.')

            messages.append(message)

        return messages

