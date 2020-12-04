import socket
import threading
import json
import uuid
import time
import jsonschema
from copy import deepcopy

import src.handshake as handshake
import src.messaging as messaging

from .side import (
    side_to_str
)
from src.connection import (
    ClientConnection
)


def accept_new_market_data_clients(config, state):
    """
    Accepts new market data subscriptions
    """
    print(f"Market data subscriptions gateway listening {config.market_data_address}:{config.market_data_port}.")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((config.market_data_address, config.market_data_port))
    s.listen(5)
    while not state.stopper.is_set():
        try:
            conn, addr = s.accept()
            print('Market data connection from:', addr)
            # Create order entry client connection
            client = ClientConnection(uuid.uuid4())
            state.add_market_data_client(client)
            client.socket = conn
            host, port = addr
            client.host = host
            client.port = port
            t = threading.Thread(
                target=handle_market_data_subscription,
                args=(state, client)
            )
            state.add_market_data_client_thread(t)
            t.start()
        except:
            pass

    s.close()

    print('Starting to close market data client threads')
    for thread in state.get_market_data_client_threads():
        thread.join()
    print(f'Stopped to accept new MD clients.')


def _create_add_message_from_order(order):
    """
    Creates add message from an order
    :param order:
    :return:
    """
    message = {}
    message.update({"message-type": "A"})
    message.update({"order-id": order.order_id})
    message.update({"price": int(order.price)})
    message.update({"quantity": int(order.quantity)})
    message.update({"side": side_to_str(order.side)})
    message.update({"timestamp": order.timestamp})
    message.update({"snapshot": 1})
    return message


def _send_order_book_snapshot(state, client):
    """
    Sends snapshot of the current state of the order book
    to the subscriber.

    """
    state.lock.acquire()
    messages = []
    lob = state.get_current_lob_state()

    # Send sell orders
    if (lob.asks is not None) and (len(lob.asks) > 0):
        for price, order_list in reversed(lob.asks.price_map.items()):
            head_order = order_list.get_head_order()
            for _ in range(0, order_list.length):
                messages.append(_create_add_message_from_order(head_order))
                head_order = head_order.next_order

    # Send buy orders
    if (lob.bids is not None) and (len(lob.bids) > 0):
        for price, order_list in reversed(lob.bids.price_map.items()):
            head_order = order_list.get_head_order()
            for _ in range(0, order_list.length):
                messages.append(_create_add_message_from_order(head_order))
                head_order = head_order.next_order

    for message in messages:
        message = json.dumps(message)
        messaging.send_data(client.socket, message, client.encoding)

    client.snapshot_sent = True

    state.lock.release()


def handle_market_data_subscription(state, client):
    """
    Handles market data subscription requests from
    clients.
    """

    # Handshake
    handshake.handshake(client.socket)
    client.handshaken = True

    # Send current snapshot of the order book
    _send_order_book_snapshot(state, client)

    # Start listening to the client market data requests
    while not state.stopper.is_set():

            request = messaging.recv_data(client.socket, 4096)
            if not request:
                break

            # TODO: Handle market data subscription messages

    print(f'Market data subscription closed for client: {client}')
    state.remove_market_data_client(client)
    client.socket.close()


def _handle_subscribe_request(state, client, request):
    """
    Handles new market data subscriptions.
    """


def _handle_unsubscribe_request(state, client, request):
    """
    Removes existing market data subscriptions
    """


market_data_message_handlers = {
    'S': _handle_subscribe_request,
    'U': _handle_unsubscribe_request
}


def public_market_data_feed(config, state):
    """
    Publishes market data events to subscribers
    """

    # Sleep until the next market event
    while not state.stopper.is_set():

        state.lock.acquire()
        while not state.event_queue.empty():

            # Get next event
            event = state.event_queue.get()

            for client in state.get_market_data_clients():
                if client.handshaken and client.snapshot_sent:
                    if not isinstance(event, dict):
                        message = event.get_message()
                        message = json.dumps(message)
                        messaging.send_data(client.socket, message, client.encoding)
                    else:
                        message = json.dumps(event)
                        messaging.send_data(client.socket, message, client.encoding)

            if state.config.display == "BOOK":
                state.get_current_lob_state().print()
            elif state.config.display == "MESSAGES":
                print(event)
        state.lock.release()

    print('Market data dispatching stopped.')
