
from IEventGenerator import (
    IEventGenerator
)

from Side import (
    SIDE
)

class ILimitOrderGenerator(IEventGenerator):
    """
    Implements common methods used by all limit order event
    generators
    """
    def __init__(self, event_type, side, arrival_rate, tick, level):
        IEventGenerator.__init__(self, event_type, side, arrival_rate, tick)
        self.level = level

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, value):
        if not isinstance(value, int):
            raise TypeError()
        self._level = value

    def _infer_price_level(self, state):
        """
        Infers correct price level based on the reference level
        and the current state of the events.
        """

        lob = state.get_current_lob_state()

        # Calculate peg, i.e. the offset w.r.t reference price
        peg = self.level * self.tick

        # Get current best bid and ask
        best_bid = lob.get_best_bid()
        best_ask = lob.get_best_ask()

        # Calculate price level
        if self.side == SIDE.B:
            if best_ask is None:  # in case ask side is empty --> peg against the best bid
                price_level = best_bid - peg
            else:
                price_level = best_ask - peg
        elif self.side == SIDE.S:
            if best_bid is None:  # in case ask side is empty --> peg against the best ask
                price_level = best_ask + peg
            else:
                price_level = best_bid + peg

        return price_level

    def create(self):
        raise NotImplementedError()
