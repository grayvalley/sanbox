import json

from datetime import datetime
from decimal import Decimal

from src.side import (
    side_to_str
)

from src.order import (
    order_type_to_str
)


def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000000.0)


class OrderEntryMessageFactory:

    @staticmethod
    def rejected_message(order, reason):

        msg = {'message-type': 'R',
               'instrument': order.instrument,
               'side': side_to_str(order.side),
               'quantity': int(order.quantity),
               'price': float(order.price),
               'timestamp': unix_time_millis(datetime.now()),
               'order-type': order_type_to_str(order.order_type),
               'reason': reason
               }

        return msg

    @staticmethod
    def canceled_message(order, reason):

        msg = {'message-type': 'X',
               'order-id': order.order_id,
               'instrument': order.instrument,
               'side': side_to_str(order.side),
               'quantity': int(order.quantity),
               'price': float(order.price),
               'timestamp': str(order.timestamp),
               'reason': reason
               }

        return msg

    @staticmethod
    def accepted_message(order):

        msg = {'message-type': 'Y',
               'instrument': order.instrument,
               'order-type': order_type_to_str(order.order_type),
               'side': side_to_str(order.side),
               'quantity': int(order.quantity),
               'price': float(order.price),
               'order-id': order.order_id,
               'timestamp': str(order.timestamp)
               }

        return msg

    @staticmethod
    def remove_message(cancel):

        msg = {'message-type': 'X',
               'order-id': cancel.order_id,
               'instrument': cancel.instrument,
               'order-type': 'LMT',
               'side': side_to_str(cancel.side),
               'price': int(cancel.price),
               'timestamp': cancel.timestamp
               }

        return msg

    @staticmethod
    def add_message(order):

        msg = {'message-type': 'A',
               'order-id': order.order_id,
               'instrument': order.instrument,
               'order-type': 'LMT',
               'quantity': int(Decimal(order.quantity)),
               'price': int(order.price),
               'side': side_to_str(order.side),
               'timestamp': order.timestamp,
               'snapshot': 0
               }

        return msg
