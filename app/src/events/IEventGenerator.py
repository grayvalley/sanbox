
from abc import (
    ABCMeta,
    abstractmethod
)

import numpy as np
from enum import Enum

from Side import (
    SIDE
)


class EventTypes(Enum):
    ADD = 1
    CANCEL = 2
    MARKET_ORDER = 3


class IEventGenerator(metaclass=ABCMeta):
    """
    Event generator interface defines methods and variables
    which are common between all event generators.
    """
    @abstractmethod
    def __init__(self, event_type, side, rate, tick):
        self.type = event_type
        self.side = side
        self.rate = rate
        self.tick = tick
        self.rng = np.random.RandomState()
        self.rng.seed(np.random.randint(1, 1000))

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        if value not in EventTypes:
            raise ValueError()
        self._type = value

    @property
    def side(self):
        return self._side

    @side.setter
    def side(self, value):
        if value not in SIDE:
            raise ValueError()
        self._side = value

    @property
    def rate(self):
        return self._rate

    @rate.setter
    def rate(self, value):
        if not isinstance(value, float):
            raise TypeError()
        if value <= 0.0:
            raise ValueError()
        self._rate = value

    @property
    def tick(self):
        return self._tick

    @tick.setter
    def tick(self, value):
        if not isinstance(value, int):
            raise TypeError()
        if value <= 0:
            raise ValueError()
        self._tick = value

    @abstractmethod
    def create(self):
        """
        Creates a new simulated event to the system.
        :return:
        """
        pass

    def sleep(self):
        """
        Sleep until the next event time.
        :return:
        """
        pass
