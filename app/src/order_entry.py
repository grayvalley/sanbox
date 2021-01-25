import socket
import threading
import json
import uuid
import jsonschema
from datetime import datetime

import src.handshake as handshake
import src.messaging as messaging

from src.side import (
    side_to_str
)

from src.order import (
    order_type_to_str
)

from src.connection import (
    ClientConnection
)

from src.soe import (
    MessageFactory
)


def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000000.0)


class OrderRequestHandler:

    def __init__(self, global_state, client):
        self._global_state = global_state
        self._client = client

        self._handlers = {}

    def handle_request(self, request):
        """
        Handle new order request.

        :param request: received request
        """
        handler = self._handlers.get(request.message_type, None)
        if handler is not None:
            handler(request)

    def cancel_order(self, request):
        """
        Handles cancel order requests

        :param request: received request
        """
        pass

    def add_or_modify_order(self, request):
        """
        Handles add or modify order requests

        :param request: received request
        """
        pass


def accept_new_order_entry_clients(config, state):

    print(f"Order entry gateway listening {config.order_entry_address}:{config.order_entry_port}.")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((config.order_entry_address, config.order_entry_port))
    s.listen(5)
    while not state.stopper.is_set():
        try:
            # Accept incoming connection request
            conn, addr = s.accept()
            print('Order entry connection from:', addr)

            # Create order entry client connection
            trader_id = uuid.uuid4()
            client = ClientConnection(trader_id)
            client.socket = conn
            host, port = addr
            client.host = host
            client.port = port
            print('Client:', addr, "has TraderId:", trader_id)

            # Add client connection to the global state
            state.add_order_client(trader_id, client)

            # Start listening to requests
            t = threading.Thread(
                target=handle_order_entry_requests,
                args=(state, client)
            )
            state.add_order_client_thread(t)
            t.start()

        except Exception as e:
            pass

    print('Closing order entry socket.')
    s.close()

    print('Stopping order client threads...')
    for thread in state.get_order_client_threads():
        thread.join()

    print('Stopped accept_new_order_entry_clients')


def handle_order_entry_requests(state, client):
    """
    Handles order entry connections and requests from
    clients.
    """

    # Implement web-socket handshake procedure
    handshake.handshake(client.socket)
    client.handshaken = True

    with open('src/etc/schema-enter-order.json', 'r') as f:
        schema_enter_order = f.read()
    with open('src/etc/schema-cancel-order.json', 'r') as f:
        schema_cancel_order = f.read()
    schema_enter_order = json.loads(schema_enter_order)
    schema_cancel_order = json.loads(schema_cancel_order)

    # Start listening to the client order entry requests
    while not state.stopper.is_set():
        try:
            # Get all requests from the socket
            requests = messaging.recv_data(client.socket, 4096)
            if not requests:
                break
            # Handle each request
            for request in requests:
                for schema in [schema_enter_order, schema_cancel_order]:
                    try:
                        jsonschema.validate(request, schema)
                    except jsonschema.exceptions.ValidationError as e:
                        pass
                    else:
                        message = MessageFactory.create(request, client.uuid)
                        handler = order_entry_message_handlers[message.message_type]
                        handler(state, client, message)

        except Exception as e:
            print(e)

    print(f'Order entry thread closed for client {client}.')

    # Cleanup
    state.remove_order_client(client.uuid)
    client.socket.close()


def _handle_order_entry_cancel_order(state, client, order):
    """
    Handles cancel order request received from a client
    and creates a response message.
    """
    state.lock.acquire()
    lob = state.get_current_lob_state(order.instrument)

    # Check if this client is the owner of the requested order id
    is_owner = order.order_id in client.orders
    if is_owner is False:
        state.lock.release()
        return

    # Try to get order from the book
    order_in_book = lob.get_order(order.order_id)

    # If order is not found, reject cancellation
    if order_in_book is None:
        state.lock.release()
        return

    # Order was found - cancel the order
    else:

        # Mark order as canceled
        client.order_set_as_canceled(order)

        # Send cancel message to the client
        cancel_message = _create_order_canceled_message(order_in_book, 'Client request.')
        messaging.send_data(client.socket, cancel_message, client.encoding)

        lob.cancel_order(order_in_book.side, order.order_id)

        # TODO: put the remove message to public data feed!
        remove_message = _create_remove_message(order_in_book)
        state.event_queue.put(remove_message)

    state.lock.release()


def _find_order_book(state, client, order):

    order_book = None
    success = False
    try:
        order_book = state.get_current_lob_state(order.instrument)
        success = True
    except KeyError as keyError:
        message = _create_order_rejected_message(order, "Invalid symbol.")
        messaging.send_data(client.socket, message, client.encoding)

    return order_book, success


def _handle_modify_order(client, order, order_book):

    # Send order accepted message
    accept_message = _create_order_accepted_message(order.to_lob_format())
    messaging.send_data(client.socket, accept_message, client.encoding)

    # Modify order iin the LOB
    order_book.modify_order(order.order_id, order.to_lob_format(), None)

    # Save order to clients open orders
    client.orders[order.order_id] = order


def can_modify_order(request, order_book):
    """
    Determine if the
    :param request:
    :param order_book:
    :return:
    """

    order = order_book.get_order(request.order_id)

    return order != None


def _handle_transaction_messages(state, client, order_in_book, transactions):

    # Generate trade messages
    aggressor_messages, passive_messages = transactions.get_trade_messages()

    # Send order executed message(s) to the passive side of the transaction
    for passive_trader_id, msg in passive_messages:
        # If passive_trader_id is None the order was simulated and no message will be sent.
        if passive_trader_id is not None:
            passive_side_client = state.get_order_client_nts(passive_trader_id)
            if passive_side_client is not None:
                messaging.send_data(
                    passive_side_client.socket,
                    json.dumps(msg),
                    passive_side_client.encoding)

    # Send order executed message(s) to the aggressing side of the transaction (client)
    for msg in aggressor_messages:
        messaging.send_data(client.socket, json.dumps(msg), client.encoding)

    # Publish trade(s) via the public market data feed
    for msg in aggressor_messages:
        state.event_queue.put(msg)

    # Publish remove and modify messages via the public market data feed
    remove_and_modify_messages = transactions.get_remove_and_modify_messages()
    for msg in remove_and_modify_messages:
        state.event_queue.put(msg)

    # Publish potential add message via the public market data feed
    if order_in_book['quantity'] > 0:
        state.event_queue.put(_create_order_stub_add_message(order_in_book))


def _handle_insert_new_order(state, client, order, order_book):

    # Insert the order to the book
    transactions, order_in_book, cancels = order_book.process_order(
        order.to_lob_format(), False, False)

    if cancels:
        _handle_self_match_prevention_cancels(state, client, cancels)

    order.order_id  = order_in_book['order_id']
    order.timestamp = order_in_book['timestamp']

    client.orders[order.order_id] = order_in_book

    messaging.send_data(client.socket, _create_order_accepted_message(order), client.encoding)

    # If the new order was matched immediately
    if not transactions.is_empty():
        _handle_transaction_messages(state, client, order_in_book, transactions)
    else:
        state.event_queue.put(order.get_message())


def _handle_self_match_prevention_cancels(state, client, cancels):

    for cancel in cancels:
        # Send OrderCanceled message to the client
        cancel_message = _create_order_canceled_message(
            cancel,
            'Order canceled due to automatic Self-Match-Prevention.'
        )
        messaging.send_data(client.socket, cancel_message, client.encoding)

        # Send remove messages trough public market data feed
        remove_message = _create_remove_message(cancel)
        state.event_queue.put(remove_message)


def _handle_order_entry_add_or_modify_order(state, client, order):
    """
    Handles order entry or modify requests.

    TODO: rejection of order logic?

    """
    state.lock.acquire()

    order_book, success = _find_order_book(state, client, order)

    if not success:
        return

    if can_modify_order(order, order_book):
        _handle_modify_order(client, order, order_book)
    else:
        _handle_insert_new_order(state, client, order, order_book)

    state.lock.release()


def _handle_order_entry_configuration(state, request):
    """
    Handles configuration request received from a client
    and creates a response message.
    """
    return "configured"


def _create_order_rejected_message(order, reason):
    """
    Creates an order rejected message from order
    """

    msg = {'message-type': 'R',
           'instrument': order.instrument,
           'side': side_to_str(order.side),
           'quantity': int(order.quantity),
           'price': float(order.price),
           'timestamp': unix_time_millis(datetime.now()),
           'order-type': order_type_to_str(order.order_type),
           'reason': reason
           }
    return json.dumps(msg)


def _create_order_canceled_message(order, reason):
    """
    Creates an order cancelled message from order
    """
    msg = {'message-type': 'X',
           'order-id': order.order_id,
           'instrument': order.instrument,
           'side': side_to_str(order.side),
           'quantity': int(order.quantity),
           'price': float(order.price),
           'timestamp': str(order.timestamp),
           'reason': reason
           }
    return json.dumps(msg)


def _create_order_accepted_message(order):
    """
    Creates an order accepted message from order
    """
    msg = {'message-type': 'Y',
           'instrument': order.instrument,
           'order-type': order_type_to_str(order.order_type),
           'side': side_to_str(order.side),
           'quantity': int(order.quantity),
           'price': float(order.price),
           'order-id': order.order_id,
           'timestamp': str(order.timestamp)
           }

    return json.dumps(msg)


def _create_remove_message(cancel):
    """
    Creates an order removed message from a Self-Match-Prevention (SMP)
    cancel.
    :param cancel: the order that was cancelled due to SMP
    :return: a cancel message
    """
    msg = {'message-type': 'X',
           'order-id': cancel.order_id,
           'instrument': cancel.instrument,
           'order-type': 'LMT',
           'side': side_to_str(cancel.side),
           'price': int(cancel.price),
           'timestamp': cancel.timestamp
           }

    return msg

def _create_order_stub_add_message(order):
    """

    :param order:
    :return:
    """
    msg = {'message-type': 'A',
           'order-id': order['order_id'],
           'order-type': 'LMT',
           'quantity': int(order['quantity']),
           'price': int(order['price']),
           'side': side_to_str(order['side']),
           'timestamp': order['timestamp'],
           'snapshot': 0
           }

    return msg


order_entry_message_handlers = {
    'C': _handle_order_entry_configuration,
    'A': _handle_order_entry_add_or_modify_order,
    'X': _handle_order_entry_cancel_order
}
