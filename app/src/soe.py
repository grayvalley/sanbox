import json

from decimal import Decimal

from .side import Side

_MESSAGE_TYPE_CONFIG = 'C'
_MESSAGE_TYPE_NEW_ORDER = 'E'
_MESSAGE_TYPE_CANCEL_ORDER = 'X'
_MESSAGE_TYPE_ORDER_ACCEPTED = 'A'
_MESSAGE_TYPE_ORDER_REJECTED = 'R'
_MESSAGE_TYPE_ORDER_EXECUTED = 'E'
_MESSAGE_TYPE_SUBSCRIBE_MARKET_DATA = 'S'
_MESSAGE_TYPE_UNSUBSCRIBE_MARKET_DATA = 'U'
_ORDER_TYPE_LIMIT_ORDER = 'LMT'
_ORDER_TYPE_MARKET_ORDER = 'MKT'


def flag_wrong_instance_type(value, wanted_type, field_name):

    if not isinstance(value, wanted_type):
        flag = False
        error_message = '{} has to be {} '.format(field_name, wanted_type)
        raise TypeError(error_message)


class MessageFactory:

    @staticmethod
    def create(request):

        message_type = request['message-type']

        message = None
        if message_type == _MESSAGE_TYPE_CONFIG:

            message = InboundConfigMessage.from_dict(request)

        elif message_type == _MESSAGE_TYPE_NEW_ORDER:

            message = InboundNewOrder.from_dict(request)

        elif message_type == _MESSAGE_TYPE_CANCEL_ORDER:

            message = InboundCancelOrder.from_dict(request)

        else:
            raise ValueError(f'Not implemented.')

        return message


class InboundConfigMessage:
    def __init__(self):
        self.message_type = _MESSAGE_TYPE_CONFIG

    @staticmethod
    def from_dict(dictionary):

        order = InboundConfigMessage()
        for key, value in dictionary.items():
            setattr(order, key.replace('-', '_'), value)
        return order


class InboundNewOrder:
    def __init__(self):
        self._message_type = None
        self._order_type = None
        self._side = None
        self._quantity = None
        self._price = None
        self._order_id = None

    @property
    def message_type(self):
        return self._message_type

    @message_type.setter
    def message_type(self, value):
        flag_wrong_instance_type(value, str, 'message_type')
        self._message_type = value

    @property
    def order_type(self):
        return self._order_type

    @order_type.setter
    def order_type(self, value):
        flag_wrong_instance_type(value, str, 'order_type')
        self._order_type = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        flag_wrong_instance_type(value, str, 'side')
        if value == 'S':
            self._side = Side.S
        elif value == 'B':
            self._side = Side.B

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        flag_wrong_instance_type(value, int, 'quantity')
        self._quantity = value

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        flag_wrong_instance_type(value, int, 'price')
        self._price = value

    @property
    def order_id(self):
        return self._order_id

    @order_id.setter
    def order_id(self, value):
        flag_wrong_instance_type(value, int, 'order_id')
        self._order_id = value

    @staticmethod
    def from_dict(dictionary):
        order = InboundNewOrder()
        for key, value in dictionary.items():
            setattr(order, key.replace('-', '_'), value)
        return order

    def to_lob_format(self):
        result = {}
        result.update({'order_type': self.order_type})
        result.update({'side': self.side})
        result.update({'quantity': Decimal(self.quantity)})
        result.update({'price': self.price})
        result.update({'order_id': self.order_id})
        return result


class InboundCancelOrder:

    def __init__(self):
        self.message_type = _MESSAGE_TYPE_CANCEL_ORDER
        self._order_type = None
        self._order_id = None
        self._quantity = None

    @property
    def order_id(self):
        return self._order_id

    @order_id.setter
    def order_id(self, value):
        flag_wrong_instance_type(value, int)
        self._order_id = value

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        flag_wrong_instance_type(value, int)
        if value <= 0:
            self.valid = False
            self.error_message = 'Quantity has to be a positive integer.'
            return
        else:
            self._quantity = value

    @staticmethod
    def from_dict(dictionary):
        order = InboundNewOrder()
        for key, value in dictionary.items():
            setattr(order, key.replace('-', '_'), value)
        return order


class OutboundOrderAccepted:

    def __init__(self):
        self._message_type = _MESSAGE_TYPE_ORDER_ACCEPTED
        self._order_type = None
        self._order_id = None
        self._side = None
        self._quantity = None
        self._price = None
        self._timestamp = None

    @property
    def order_type(self):
        return self._order_type

    @order_type.setter
    def order_type(self, value):
        flag_wrong_instance_type(value, str)

        if value not in [_ORDER_TYPE_LIMIT_ORDER, _ORDER_TYPE_MARKET_ORDER]:
            raise ValueError(f'order_type has to be LMT or MKT, was {value}.')
        self._order_type = value

    @property
    def order_id(self):
        return self._order_id

    @order_id.setter
    def order_id(self, value):
        flag_wrong_instance_type(value, int)
        self._order_id = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        flag_wrong_instance_type(value, str)

        if value == 'S':
            self._side = Side.ASK
        elif value == 'B':
            self._side = Side.BID
        else:
            raise ValueError(f'Side has to be S or B, was {type(value)}.')

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        flag_wrong_instance_type(value, int)
        if value <= 0:
            raise ValueError(f'Quantity has to be positive, was {type(value)}.')
        else:
            self._quantity = value

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        flag_wrong_instance_type(value, int)
        if value <= 0:
            raise ValueError(f'Price has to be positive, was {type(value)}.')
        else:
            self._price = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        flag_wrong_instance_type(value, int)
        self._timestamp = value


class OutboundOrderRejected:

    def __init__(self):
        self._message_type = _MESSAGE_TYPE_ORDER_REJECTED
        self._order_type = None
        self._side = None
        self._quantity = None
        self._price = None
        self._timestamp = None
        self._reason = None

    @property
    def order_type(self):
        return self._order_type

    @order_type.setter
    def order_type(self, value):
        flag_wrong_instance_type(value, str)

        if value not in [_ORDER_TYPE_LIMIT_ORDER, _ORDER_TYPE_MARKET_ORDER]:
            raise ValueError(f'order_type has to be LMT or MKT, was {type(value)}')
        self._order_type = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        flag_wrong_instance_type(value, int)

        if value == 'S':
            self._side = Side.ASK
        elif value == 'B':
            self._side = Side.BID
        else:
            raise ValueError(f'Side has to be positive S or B.')
        self._side = value

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        flag_wrong_instance_type(value, int)

        if value <= 0:
            raise ValueError(f'Price has to be positive.')
        else:
            self._quantity = value

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        flag_wrong_instance_type(value, int)

        if value <= 0:
            raise ValueError(f'Price has to be positive.')
        else:
            self._price = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        flag_wrong_instance_type(value, int)

        self._timestamp = value

    @property
    def reason(self):
        return self._reason

    @reason.setter
    def reason(self, value):
        flag_wrong_instance_type(value, int)

        if value not in ['P', 'Q']:
            raise ValueError(f'Reason has to be either P or Q, was {value}')
        self._reason = value

    def to_dict(self):

        result = {}
        result.update({'message-type': self.message_type})
        result.update({'order-type': self.order_type})
        result.update({'side': self.side})
        result.update({'quantity': self.quantity})
        result.update({'price': self.price})
        result.update({'timestamp': self.timestamp})
        result.update({'reason': self.reason})

        return result

    def __str__(self):
        """
        String operator overload
        """
        return json.dumps(self.to_dict())


class OutboundOrderExecuted:

    def __init__(self):
        self._message_type = _MESSAGE_TYPE_ORDER_EXECUTED
        self._order_type = None
        self._order_id = None
        self._side = None
        self._quantity = None
        self._price = None
        self._timestamp = None
        self._liquidity = None

    @property
    def order_type(self):
        return self._order_type

    @order_type.setter
    def order_type(self, value):
        flag_wrong_instance_type(value, str)

        if value not in [_ORDER_TYPE_LIMIT_ORDER, _ORDER_TYPE_MARKET_ORDER]:
            raise ValueError(f'order_type has to be LMT or MKT, was {type(value)}')
        self._order_type = value

    @property
    def order_id(self):
        return self._order_id

    @order_id.setter
    def order_id(self, value):
        flag_wrong_instance_type(value, int)

        self._order_id = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        flag_wrong_instance_type(value, str)
        if value == 'S':
            self._side = Side.ASK
        elif value == 'B':
            self._side = Side.BID
        else:
            raise ValueError(f'Side has to be positive S or B.')
        self._side = value

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        flag_wrong_instance_type(value, int)

        if value <= 0:
            raise ValueError(f'Price has to be positive.')
        else:
            self._quantity = value

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        flag_wrong_instance_type(value, int)

        if value <= 0:
            raise ValueError(f'Price has to be positive.')
        else:
            self._price = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        flag_wrong_instance_type(value, int)

        self._timestamp = value

    @property
    def liquidity(self):
        return self._liquidity

    @liquidity.setter
    def liquidity(self, value):
        flag_wrong_instance_type(value, int)

        if value not in ['A', 'R']:
            raise ValueError(f'Liquidity has to be either A or R, was {value}')
        self._liquidity = value


class OutboundOrderCanceled:

    def __init__(self):

        self._message_type = _MESSAGE_TYPE_CANCEL_ORDER
        self._order_type = None
        self._order_id = None
        self._side = None
        self._quantity = None
        self._price = None
        self._timestamp = None

    @property
    def order_type(self):
        return self._order_type

    @order_type.setter
    def order_type(self, value):
        flag_wrong_instance_type(value, str)

        if value not in [_ORDER_TYPE_LIMIT_ORDER]:
            raise ValueError(f'order_type has to be LMT, {type(value)}.')
        self._order_type = value

    @property
    def order_id(self):
        return self._order_id

    @order_id.setter
    def order_id(self, value):
        flag_wrong_instance_type(value, str)

        self._order_id = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        flag_wrong_instance_type(value, str)

        if value == 'S':
            self._side = Side.ASK
        elif value == 'B':
            self._side = Side.BID
        else:
            raise ValueError(f'Side has to be positive S or B.')
        self._side = value

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        flag_wrong_instance_type(value, int)

        if value <= 0:
            raise ValueError(f'Price has to be positive.')
        else:
            self._quantity = value

    @property
    def price(self):
        return self._price

    @price.setter
    def price(self, value):
        flag_wrong_instance_type(value, int)

        if value <= 0:
            raise ValueError(f'Price has to be positive.')
        else:
            self._price = value

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        flag_wrong_instance_type(value, int)
        self._timestamp = value

