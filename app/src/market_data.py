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


def _send_order_book_snapshot(state, client, symbol):
    """
    Sends snapshot of the current state of the order book
    to the subscriber.

    """
    state.lock.acquire()

    # Try to find order book corresponding to symbol
    try:
        lob = state.get_current_lob_state(symbol)
    except KeyError as exc:
        state.lock.release()
        return

    messages = []
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

    # Start listening to the client market data requests
    while not state.stopper.is_set():

            requests = messaging.recv_data(client.socket, 4096)
            if not requests:
                break

            # Handle market data subscription messages
            for request in requests:
                op = request.get('op', None)
                if op is not None:
                    try:
                        handler = market_data_message_handlers[op]
                        handler(state, client, request)
                    except KeyError as exc:
                        # client send something with wrong key
                        pass

    print(f'Market data subscription closed for client: {client}')
    state.remove_market_data_client(client)
    client.socket.close()


def _handle_subscribe_request(state, client, request):
    """
    Handles new market data subscriptions.

    Order book snapshot is always sent at the beginning of each subscription.
    """
    args = request["args"]
    for arg in args:
        topic, symbol = arg.split(":")
        symbol = int(symbol)
        client.add_market_data_subscription(topic, symbol)
        if topic == 'orderBookL2':
            _send_order_book_snapshot(state, client, symbol)
        elif topic == 'trade':
            # TODO: send trade snapshot
            pass
        else:
            # TODO: send not valid topic
            pass


def _handle_unsubscribe_request(state, client, request):
    """
    Removes existing market data subscriptions
    """
    # TODO: create me


market_data_message_handlers = {
    'subscribe': _handle_subscribe_request,
    'unsubscribe': _handle_unsubscribe_request
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

            # TODO: ugly
            if isinstance(event, dict):
                symbol = event['instrument']
                message_type = event['message-type']
            else:
                symbol = event.instrument
                message_type = event.message_type

            for client in state.get_market_data_clients():
                if client.handshaken and client.snapshot_sent:
                    subscriptions = client.subscriptions
                    if symbol in subscriptions:
                        topics = client.subscriptions[symbol]
                        if message_type in ['A', 'X', 'M']:
                            if 'orderBookL2' in topics:
                                if not isinstance(event, dict):
                                    message = event.get_message()
                                    messaging.send_data(client.socket, message, client.encoding)
                                else:
                                    message = json.dumps(event)
                                    messaging.send_data(client.socket, message, client.encoding)

                        elif message_type in ['E']:
                            if 'trade' in topics:
                                if not isinstance(event, dict):
                                    message = event.get_message()
                                    messaging.send_data(client.socket, message, client.encoding)
                                else:
                                    message = json.dumps(event)
                                    messaging.send_data(client.socket, message, client.encoding)

            state.get_current_lob_state(event['instrument']).print()

        state.lock.release()

    print('Market data dispatching stopped.')
