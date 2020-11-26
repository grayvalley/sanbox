# Module responsible for establishing websocket handhake

import base64
import hashlib

def create_hash (key):

	# Create hash key for websocket handshake
	magic_string = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	sha1 = hashlib.sha1((key + magic_string).encode('utf-8'))
	return base64.b64encode(sha1.digest())

def parse_headers (data):
	# Parse received client header to key : value pair
	headers = {}

	data = data.decode('utf-8')

	lines = data.splitlines()
	for l in lines:
		parts = l.split(": ", 1)
		if len(parts) == 2:
			headers[parts[0]] = parts[1]
	return headers

def create_response (headers):
	hash_key = create_hash(headers['Sec-WebSocket-Key'])
	response = "HTTP/1.1 101 WebSocket Protocol Handshake\r\n"
	response += "Upgrade: %s\r\n" % headers['Upgrade']
	response += "Connection: %s\r\n" % headers['Connection']
	response += "Sec-WebSocket-Accept: %s\r\n" % hash_key
	if 'Sec-WebSocket-Protocol' in headers:
		response += "Sec-WebSocket-Protocol: %s\r\n\r\n" % headers['Sec-WebSocket-Protocol']
	return response

def handshake (client):
	"""
	Establish websocket connection
	"""

	print('Handshaking...')
	data = client.recv(4096)
	headers = parse_headers(data)
	print('Got headers:')
	for k, v in headers.items():
		print(k, ':', v)
	response = create_response(headers)
	print('Sending response %s' % response)
	return client.send(response.encode('utf-8'))
