import socket
import threading
import json
import jsonschema
import src.handshake as handshake
import src.messaging as messaging

from src.side import (
    side_to_str
)

from src.connection import (
    ClientConnection
)

from src.soe import (
    MessageFactory
)


def accept_new_order_entry_clients(config, state):

    print(f"Order entry listening {config.order_entry_address}:{config.order_entry_port}.")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((config.order_entry_address, config.order_entry_port))
    s.listen(5)
    while not state.stopper.is_set():
        try:
            conn, addr = s.accept()
            print('Order entry connection from:', addr)
            state.add_order_client(conn)
            # Create order entry client connection
            client = ClientConnection()
            client.socket = conn
            host, port = addr
            client.host = host
            client.port = port
            t = threading.Thread(
                target=handle_order_entry_requests,
                args=(state, client)
            )
            state.add_order_client_thread(t)
            t.start()
        except:
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

    # Start listening to the client order entry requests
    while not state.stopper.is_set():

        request = messaging.recv_data(client.socket, 4096)
        if not request:
            break

        # TODO: Read at start up
        with open('src/etc/schema-enter-order.json', 'r') as f:
            schema_enter_order = f.read()
        with open('src/etc/schema-cancel-order.json', 'r') as f:
            schema_cancel_order = f.read()

        schema_enter_order = json.loads(schema_enter_order)
        schema_cancel_order = json.loads(schema_cancel_order)

        for schema in [schema_enter_order, schema_cancel_order]:
            try:
                jsonschema.validate(request, schema)
            except jsonschema.exceptions.ValidationError as e:
                pass
            else:
                message = MessageFactory.create(request)
                handler = order_entry_message_handlers[message.message_type]
                handler(state, client, message)

    print(f'Order entry thread closed for client {client}.')

    # Cleanup
    state.remove_order_client(client.socket)
    client.socket.close()


def _handle_order_entry_add_or_modify_order(state, client, order):
    """
    Handles add order to LOB request received from a client
    and creates a response message.

    TODO: rejection of order logic?

    """
    state.lock.acquire()
    lob = state.get_current_lob_state()
    lob.print()
    if order.order_id is not None:
        accept_message = _create_order_accepted_message(order.to_lob_format())
        messaging.send_data(client.socket, accept_message, client.encoding)
        lob.modify_order(order.order_id, order.to_lob_format(), None)
    else:
        transactions, order_in_book = lob.process_order(
            order.to_lob_format(), False, False)
        accept_message = _create_order_accepted_message(order_in_book)
        messaging.send_data(client.socket, accept_message, client.encoding)

        if not transactions.is_empty():

            trade_messages = transactions.get_trade_messages()

            # Send order executed message to client
            for msg in trade_messages:
                messaging.send_data(client.socket, json.dumps(msg), client.encoding)

            # Send trades via public market data feed
            for msg in trade_messages:
                state.event_queue.put(msg)

            # Send remove and modify messages via public market data feed
            remove_and_modify_messages = transactions.get_remove_and_modify_messages()
            for msg in remove_and_modify_messages:
                state.event_queue.put(msg)
        else:
            state.event_queue.put(order)

    state.lock.release()


def _handle_order_entry_configuration(state, request):
    """
    Handles configuration request received from a client
    and creates a response message.
    """
    return "configured"


def _create_order_accepted_message(order):
    """
    Creates order accepted message from order
    """
    msg = {'message-type': 'Y',
           'order-type': order['order_type'],
           'side': side_to_str(order['side']),
           'quantity': int(order['quantity']),
           'price': float(order['price']),
           'order-id': order['order_id'],
           'timestamp': str(order['timestamp'])
           }
    return json.dumps(msg)


def _handle_order_entry_cancel_order(state, request):
    """
    Handles cancel order request received from a client
    and creates a response message.
    """
    return "order cancelled"


order_entry_message_handlers = {
    'C': _handle_order_entry_configuration,
    'E': _handle_order_entry_add_or_modify_order,
    'X': _handle_order_entry_cancel_order
}
