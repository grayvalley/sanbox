
from ILimitOrderGenerator import (
    ILimitOrderGenerator
)


class LimitOrderAddGenerator(ILimitOrderGenerator):
    """
    Creates new limit order add events
    """
    def __init__(self, event_type, side, arrival_rate, tick, level):
        ILimitOrderGenerator.__init__(self, event_type, side, arrival_rate, tick)
        self._level = level

    def create(self):
        pass
