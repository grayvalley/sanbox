import socket
import threading
import json
import uuid
import jsonschema
from datetime import datetime

import src.handshake as handshake
import src.messaging as messaging

from src.order_entry_messaging import (
    OrderEntryMessageFactory
)

from src.connection import (
    ClientConnection
)

from src.soe import (
    MessageFactory
)


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


def create_order_entry_socket(config):

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((config.order_entry_address, config.order_entry_port))
    sock.listen(5)

    return sock


def accept_new_order_entry_clients(config, state):

    print(f"Order entry gateway listening {config.order_entry_address}:{config.order_entry_port}.")

    sock = create_order_entry_socket(config)

    while not state.stopper.is_set():
        try:
            # Accept incoming connection request
            conn, addr = sock.accept()
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
    sock.close()

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


def is_owner(order, client):
    return order.order_id in client.orders


def _handle_order_entry_cancel_order(state, client, order):
    """
    Handles cancel order requests
    """
    state.lock.acquire()

    lob = state.get_current_lob_state(order.instrument)

    if not is_owner(order, client):
        state.lock.release()
        return

    order_in_book = lob.get_order(order.order_id)

    if order_in_book is None:
        # TODO: send order cancel rejected
        state.lock.release()
        return
    else:
        lob.cancel_order(order_in_book.side, order.order_id)
        client.order_set_as_canceled(order)

        messaging.send_data(
            client.socket,
            OrderEntryMessageFactory.canceled_message(order_in_book, 'Client request.'),
            client.encoding)

        state.event_queue.put(OrderEntryMessageFactory.remove_message(order_in_book))

    state.lock.release()


def _find_order_book(state, client, order):

    order_book = None
    success = False
    try:
        order_book = state.get_current_lob_state(order.instrument)
        success = True
    except KeyError as keyError:
        message = OrderEntryMessageFactory.rejected_message(order, "Invalid symbol.")
        messaging.send_data(client.socket, message, client.encoding)

    return order_book, success


def _handle_modify_order(client, order, order_book):

    messaging.send_data(
        client.socket,
        OrderEntryMessageFactory.accepted_message(order.to_lob_format()),
        client.encoding)

    order_book.modify_order(order.order_id, order.to_lob_format(), None)

    # Save order to clients open orders
    client.orders[order.order_id] = order

    # TODO: Publish order modify message to public market data feed


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
        state.event_queue.put(MessageFactory(order_in_book))


def _handle_insert_new_order(state, client, order, order_book):

    # Insert the order to the book
    transactions, order_in_book, cancels = order_book.process_order(
        order.to_lob_format(), False, False)

    if cancels:
        _handle_self_match_prevention_cancels(state, client, cancels)

    order.order_id = order_in_book['order_id']
    order.timestamp = order_in_book['timestamp']

    client.orders[order.order_id] = order_in_book

    accepted_message = OrderEntryMessageFactory.accepted_message(order)
    messaging.send_data(client.socket, json.dumps(accepted_message) , client.encoding)

    # If the new order was matched immediately
    if not transactions.is_empty():
        _handle_transaction_messages(state, client, order_in_book, transactions)
    else:
        add_messge = OrderEntryMessageFactory.add_message(order)
        state.event_queue.put(add_messge)


def _handle_self_match_prevention_cancels(state, client, cancels):

    for cancel in cancels:

        cancel_message = OrderEntryMessageFactory.canceled_message(
            cancel,
            'Order canceled due to automatic Self-Match-Prevention.'
        )

        messaging.send_data(client.socket, json.dumps(cancel_message), client.encoding)

        state.event_queue.put(cancel_message)


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





order_entry_message_handlers = {
    'C': _handle_order_entry_configuration,
    'A': _handle_order_entry_add_or_modify_order,
    'X': _handle_order_entry_cancel_order
}
