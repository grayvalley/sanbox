import socket


class ClientConnection:

    def __init__(self, id_):
        self._socket = None
        self._host = None
        self._port = None
        self._encoding = 1
        self._handshaken = False
        self._snapshot_sent = False
        self._orders = {}
        self._uuid = id_

    @property
    def uuid(self):
        return self._uuid

    @property
    def orders(self):
        return self._orders

    @property
    def encoding(self):
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        if not isinstance(value, int):
            raise TypeError(f"Encoding has to be type of <int>, was {type(int)}.")
        self._encoding = value

    @property
    def socket(self):
        return self._socket

    @socket.setter
    def socket(self, value):
        if not isinstance(value, socket.socket):
            raise TypeError(f'Socket has to be type of <socket.socket>, was {type(value)}')
        self._socket = value

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, value):
        if not isinstance(value, str):
            raise TypeError(f'Host has to be type of <str>, was {type(value)}')
        self._host = value

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, value):
        if not isinstance(value, int):
            raise TypeError(f'Host has to be type of <int>, was {type(value)}')
        self._port = value

    @property
    def handshaken(self):
        return self._handshaken

    @handshaken.setter
    def handshaken(self, value):
        if not isinstance(value, int):
            raise TypeError(f'Handshaken has to be type of <bool>, was {type(value)}')
        self._handshaken = value

    @property
    def snapshot_sent(self):
        return self._snapshot_sent

    @snapshot_sent.setter
    def snapshot_sent(self, value):
        if not isinstance(value, int):
            raise TypeError(f'Snapshot_sent has to be type of <bool>, was {type(value)}')
        self._snapshot_sent = value

    def __str__(self):
        return f'{self.host}:{self.port}'

