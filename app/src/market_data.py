import socket
import threading
import json
import uuid
import time
import jsonschema
import src.handshake as handshake
import src.messaging as messaging

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


def handle_market_data_subscription(state, client):
    """
    Handles market data subscription requests from
    clients.
    """
    handshake.handshake(client.socket)
    client.handshaken = True

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

        while not state.event_queue.empty():

            # Get next event
            event = state.event_queue.get()

            for client in state.get_market_data_clients():
                if client.handshaken:
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

    print('Market data dispatching stopped.')
