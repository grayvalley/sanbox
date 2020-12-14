
from ILimitOrderGenerator import (
    ILimitOrderGenerator
)


class LimitOrderCancelGenerator(ILimitOrderGenerator):
    """
    Creates new limit order cancel events
    """
    def __init__(self, event_type, side, arrival_rate, tick, level):
        ILimitOrderGenerator.__init__(self, event_type, side, arrival_rate, tick)
        self._level = level

    def create(self):
        pass
