from enum import Enum
from decimal import Decimal
from datetime import datetime
from .side import (
    Side,
    side_to_str
)


class EventTypes(Enum):
    ADD = 1
    CANCEL = 2
    MARKET_ORDER = 3


class Event:

    def __init__(self, event_type):
        self._event_type = event_type
        self._side = None
        self._trade_id = None
        self._order_id = None
        self._timestamp = datetime.now()

    @property
    def trade_id(self):
        return self._trade_id

    @trade_id.setter
    def trade_id(self, value):
        if not isinstance(value, int):
            raise TypeError(f'Id has to be <int>, was {type(value)}.')
        self._trade_id = value

    @property
    def order_id(self):
        return self._trade_id

    @order_id.setter
    def order_id(self, value):
        if not isinstance(value, (int, type(None))):
            raise TypeError(f'Id has to be <int>, was {type(value)}.')
        self._trade_id = value

    @property
    def dt_timestamp(self):
        return self._timestamp
    
    @property
    def str_timestamp(self):
        return self._timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')

    @property
    def event_type(self):
        return self._event_type

    @event_type.setter
    def event_type(self, value):
        if value not in [EventTypes.ADD, EventTypes.CANCEL, EventTypes.MARKET_ORDER]:
            raise ValueError("Event type not understood.")
        self._event_type = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        if value not in [Side.B, Side.S]:
            raise ValueError("Event side not understood.")
        self._side = value

    def to_lob_format(self):
        raise NotImplementedError("This needs to be overridden.")


class Add(Event):

    def __init__(self):
        Event.__init__(self, EventTypes.ADD)
        self._price = None
        self._quantity = None

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        if not isinstance(value, Decimal):
            raise TypeError(f"Price needs to be <Decimal, was {type(value)}.")
        if value <= 0:
            raise ValueError(f"Price has to be positive <Decimal>.")
        self._price = value

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        if not isinstance(value, Decimal):
            raise TypeError(f"Quantity needs to be <Decimal, was {type(value)}.")
        if value <= 0:
            raise ValueError(f"Quantity has to be positive <Decimal>.")
        self._quantity = value

    def to_lob_format(self):
        """
        Transforms the event object into format that is understood
        by the the OrderBook class.
        """
        result = {}
        result.update({'order_type': 'LMT'})
        result.update({'price': self.price})
        result.update({'side': self.side})
        result.update({'quantity': self.quantity})

        return result

    def get_message(self):
        """
        Transforms the event object into SDM format.
        """
        message = {}
        message.update({'message-type': 'A'})
        message.update({'order-id': self.order_id})
        message.update({'order-type': 'LMT'})
        message.update({'quantity': int(self.quantity)})
        message.update({'price': int(self.price)})
        message.update({'side': side_to_str(self.side)})
        message.update({'timestamp': self.str_timestamp})

        return message


class Cancel(Event):

    def __init__(self):
        Event.__init__(self, EventTypes.CANCEL)
        self._price = None
        self._quantity = None

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        if not isinstance(value, Decimal):
            raise TypeError(f"Price needs to be <Decimal, was {type(value)}.")
        if value <= 0:
            raise ValueError(f"Price has to be positive <Decimal>.")
        self._price = value

    def to_lob_format(self):
        """
        Transforms the event object into format that is understood
        by the the OrderBook class.
        """
        result = {}
        result.update({'order_id': self.order_id})
        result.update({'type': 'cancel'})
        result.update({'side': self.side})
        result.update({'timestamp': self.str_timestamp})

        return result

    def get_message(self):

        message = {}
        message.update({'message-type': 'X'})
        message.update({'timestamp': self.str_timestamp})
        message.update({'order-id': self.order_id})

        return message


class MarketOrder(Event):

    def __init__(self):
        Event.__init__(self, EventTypes.MARKET_ORDER)
        self._quantity = None

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        if not isinstance(value, Decimal):
            raise TypeError(f"Quantity needs to be <Decimal, was {type(value)}.")
        if value <= 0:
            raise ValueError(f"Quantity has to be positive <Decimal>.")
        self._quantity = value

    def to_lob_format(self):
        """
        Transforms the event object into format that is understood
        by the the OrderBook class.
        """
        result = {}

        result.update({'order_type': 'MKT'})
        result.update({'side': self.side})
        result.update({'quantity': self.quantity})

        return result


