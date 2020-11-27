import socket
import threading
import json
import uuid
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

    print(f"Order entry gateway listening {config.order_entry_address}:{config.order_entry_port}.")

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
            client = ClientConnection(uuid.uuid4())
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
                message = MessageFactory.create(request, client.uuid)
                handler = order_entry_message_handlers[message.message_type]
                handler(state, client, message)

    print(f'Order entry thread closed for client {client}.')

    # Cleanup
    state.remove_order_client(client.socket)
    client.socket.close()


def _handle_order_entry_cancel_order(state, client, order):
    """
    Handles cancel order request received from a client
    and creates a response message.
    """
    state.lock.acquire()
    lob = state.get_current_lob_state()

    # Check if this client is the owner of the requested order id
    is_owner = order.order_id in client.orders

    # Look up order from the book
    if is_owner is False:
        rejected_message = _create_order_rejected_message(order, 'Not your order.')
        messaging.send_data(client.socket, rejected_message, client.encoding)
        return
    order_in_book = lob.get_order(order.order_id)
    # If order is not found, reject cancellation
    if order_in_book is None:
        rejected_message = _create_order_rejected_message(order, 'OrderId not found.')
        messaging.send_data(client.socket, rejected_message, client.encoding)
    # Order was found
    else:
        cancel_message = _create_order_canceled_message(order_in_book, 'Client request.')
        messaging.send_data(client.socket, cancel_message, client.encoding)
        lob.cancel_order(order_in_book.side, order.order_id)
    lob.print()
    state.lock.release()


def _handle_order_entry_add_or_modify_order(state, client, order):
    """
    Handles add order to LOB request received from a client
    and creates a response message.

    TODO: rejection of order logic?

    """
    state.lock.acquire()
    lob = state.get_current_lob_state()

    # Modification
    if order.order_id is not None:

        # Send order accepted message
        accept_message = _create_order_accepted_message(order.to_lob_format())
        messaging.send_data(client.socket, accept_message, client.encoding)

        # Modify order iin the LOB
        lob.modify_order(order.order_id, order.to_lob_format(), None)

        # Save order to clients open orders
        client.orders[order.order_id] = order

    # New order
    else:
        # We do matching first because the order is new and we
        # need to generate order id for it.
        transactions, order_in_book, smp_cancels = lob.process_order(
            order.to_lob_format(), False, False)

        # Send Self-Match-Prevention cancels, if any
        if smp_cancels != []:
            for cancel in smp_cancels:
                cancel_message = _create_order_canceled_message(
                            cancel, 'Order canceled due to automatic Self-Match-Prevention.')
                messaging.send_data(client.socket, cancel_message, client.encoding)

        # Send order accepted message
        # TODO: make this prettier
        order.order_id = order_in_book['order_id']
        order.timestamp = order_in_book['timestamp']
        accept_message = _create_order_accepted_message(order)
        messaging.send_data(client.socket, accept_message, client.encoding)

        # If the new order was matched immediately
        if not transactions.is_empty():

            # Save the order to the clients state
            client.orders[order.order_id] = order_in_book

            # Generate trade messages
            trade_messages = transactions.get_trade_messages()

            # Send order executed message(s) to client
            for msg in trade_messages:
                messaging.send_data(client.socket, json.dumps(msg), client.encoding)

            # Publish trade(s) via the public market data feed
            for msg in trade_messages:
                state.event_queue.put(msg)

            # Publish remove and modify messages via the public market data feed
            remove_and_modify_messages = transactions.get_remove_and_modify_messages()
            for msg in remove_and_modify_messages:
                state.event_queue.put(msg)

        # If the new order was just placed in the book
        else:

            # Save the order to the clients state
            client.orders[order.order_id] = order_in_book

            # Publish the event via the public market data feed
            # TODO: make this prettier
            order.order_id = order_in_book['order_id']
            order.timestamp = order_in_book['timestamp']
            state.event_queue.put(order)

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
           'order-id': order.order_id,
           'reason': reason
           }
    return json.dumps(msg)


def _create_order_canceled_message(order, reason):
    """
    Creates an order cancelled message from order
    """
    msg = {'message-type': 'X',
           'order-id': order.order_id,
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
           'order-type': order.order_type,
           'side': side_to_str(order.side),
           'quantity': int(order.quantity),
           'price': float(order.price),
           'order-id': order.order_id,
           'timestamp': str(order.timestamp)
           }
    return json.dumps(msg)


order_entry_message_handlers = {
    'C': _handle_order_entry_configuration,
    'E': _handle_order_entry_add_or_modify_order,
    'X': _handle_order_entry_cancel_order
}
