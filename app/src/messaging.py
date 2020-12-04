import json
import struct
import errno
from socket import error as SocketError


def recv_data(client, length):
	"""
	receive data from websocket
	:param client: socket.
	:param length: data length.
	:return: decoded message
	"""
	message = None
	try:
		data = client.recv(length)
	except SocketError as e:
		if e.errno == errno.ECONNRESET:
			return None

	try:
		message = json.loads(data.decode('utf-8'))
		print("Message received:", message)
		return message
	except Exception as e:
		pass

	try:
		data_length = determine_length(data)
		while data_length > length:
			data += client.recv(length)
			data_length -= length
		message = json.loads(decode_char(data, data_length))
		print("Message received:", message)
		return message
	except Exception as e:
		pass

	return None


def determine_length(data):
	"""
	determine message length from first bytes
	:param data: websocket frame
	:return:
	"""
	byte_array = [d for d in data[:10]]
	data_length = byte_array[1] & 127
	if data_length == 126:
		data_length = (byte_array[2] & 255) * 2**8
		data_length += byte_array[3] & 255
	elif data_length == 127:
		data_length = (byte_array[2] & 255) * 2**56
		data_length += (byte_array[3] & 255) * 2**48
		data_length += (byte_array[4] & 255) * 2**40
		data_length += (byte_array[5] & 255) * 2**32
		data_length += (byte_array[6] & 255) * 2**24
		data_length += (byte_array[7] & 255) * 2**16
		data_length += (byte_array[8] & 255) * 2**8
		data_length += byte_array[9] & 255
	return data_length


def decode_char(data, data_length):
	"""
	turn byte data into string
	:param data:
	:param data_length:
	:return:
	"""
	byte_array = [character for character in data]
	index_first_mask = 2

	if data_length >= 126 and data_length <= 65535:
		index_first_mask = 4
	elif data_length > 65535:
		index_first_mask = 10

	masks = [m for m in byte_array[index_first_mask : index_first_mask + 4]]
	index_first_data = index_first_mask + 4
	decoded_chars = []
	i = index_first_data
	j = 0

	while i < len(byte_array):
		decoded_chars.append(chr(byte_array[i] ^ masks[j % 4]))
		i += 1
		j += 1

	return ''.join(decoded_chars)


def send_data(client, data, encoding):
	msg = None
	if encoding == 0:
		msg = data.econde('utf-8')
	else:
		msg = encode_char(data)

	if msg is not None:
		return client.send(msg)


def encode_char(string):
	"""
	turn string values into opererable numeric byte values
	:param string:
	:return:
	"""

	byte_array = [ord(character) for character in string]
	data_length = len(byte_array)
	bytes_formatted = [129]

	if data_length <= 125:
		bytes_formatted.append(data_length)

	elif data_length >= 126 and data_length <= 65535:
		bytes_formatted.append(126)
		bytes_formatted.append((data_length >> 8) & 255)
		bytes_formatted.append(data_length & 255)
	else:
		bytes_formatted.append(127)
		bytes_formatted.append((data_length >> 56) & 255)
		bytes_formatted.append((data_length >> 48) & 255)
		bytes_formatted.append((data_length >> 40) & 255)
		bytes_formatted.append((data_length >> 32) & 255)
		bytes_formatted.append((data_length >> 24) & 255)
		bytes_formatted.append((data_length >> 16) & 255)
		bytes_formatted.append((data_length >> 8) & 255)
		bytes_formatted.append(data_length & 255)

	bytes_formatted.extend(byte_array)

	return struct.pack('B'*len(bytes_formatted), *bytes_formatted)


def create_add_message(event):
	"""
	Generates a add message that is broad-casted to all
	market data subscribers.
	"""

	message = {}
	message.update({'message-type': 'A'})
	message.update({'timestamp': event['timestamp']})
	message.update({'order-number': event['order_id']})

	if event['side'] == 'bid':
		message.update({'side': 'B'})
	else:
		message.update({'side': 'S'})

	message.update({'quantity': event['quantity']})
	message.update({'price': int(event['price'])})

	return message

