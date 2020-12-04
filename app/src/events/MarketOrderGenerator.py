
from IEventGenerator import (
    IEventGenerator
)


class MarketOrderGenerator(IEventGenerator):
    """
    Creates new market order events
    """
    def __init__(self, event_type, side, arrival_rate, tick):
        IEventGenerator.__init__(self, event_type, side, arrival_rate, tick)

    def create(self):
        pass
