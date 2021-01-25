import threading
import numpy as np

from src.side import (
    Side
)
from src.event_generator import (
    EventTypes,
    EventGenerator,
    event_generation_loop
)


def run_market_data_simulation(config, state):

    # Create threads that create the market events
    threads = []

    # Create buy limit order add sampling threads
    n_levels = 15
    thread_id = 1
    instrument = "0"
    for level in range(1, n_levels + 1):
        generator = EventGenerator(thread_id, instrument, EventTypes.ADD, Side.B, level, 1.10 * np.exp(-0.08*(level - 1)), 1)
        kwargs = {'state': state, 'generator': generator}
        state.add_simulation_thread(threading.Thread(target=event_generation_loop, kwargs=kwargs))
        thread_id += 1

    # Create sell limit order add sampling threads
    for level in range(1, n_levels + 1):
        generator = EventGenerator(thread_id, instrument, EventTypes.ADD, Side.S, level, 1.10 * np.exp(-0.08*(level - 1)), 1)
        kwargs = {'state': state, 'generator': generator}
        state.add_simulation_thread(threading.Thread(target=event_generation_loop, kwargs=kwargs))
        thread_id += 1

    # Create buy limit order cancel sampling threads
    for level in range(1, n_levels + 1):
        generator = EventGenerator(thread_id, instrument, EventTypes.CANCEL, Side.B, level, 1.0 * np.exp(-0.10*(level - 1)), 1)
        kwargs = {'state': state, 'generator': generator}
        state.add_simulation_thread(threading.Thread(target=event_generation_loop, kwargs=kwargs))
        thread_id += 1

    # Create sell limit order cancel sampling threads
    for level in range(1, n_levels + 1):
        generator = EventGenerator(thread_id, instrument, EventTypes.CANCEL, Side.S, level, 1.0 * np.exp(-0.10*(level - 1)), 1)
        kwargs = {'state': state, 'generator': generator}
        state.add_simulation_thread(threading.Thread(target=event_generation_loop, kwargs=kwargs))
        thread_id += 1

    # Create a buy market order sampling thread
    generator = EventGenerator(thread_id, instrument, EventTypes.MARKET_ORDER, Side.B, None, 0.5, None)
    kwargs = {'state': state, 'generator': generator}
    state.add_simulation_thread(threading.Thread(target=event_generation_loop, kwargs=kwargs))
    thread_id += 1

    # Create a sell market order sampling thread
    generator = EventGenerator(thread_id, instrument, EventTypes.MARKET_ORDER, Side.S, None, 0.5, None)
    kwargs = {'state': state, 'generator': generator}
    state.add_simulation_thread(threading.Thread(target=event_generation_loop, kwargs=kwargs))
    thread_id += 1

    # Start the threads
    for thread in state.get_simulation_threads():
        thread.start()
