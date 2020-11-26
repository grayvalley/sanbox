from enum import Enum


class Side(Enum):
    B = 1
    S = 2


def side_to_str(side):
    if side == Side.B:
        return 'B'
    elif side == Side.S:
        return 'S'
    else:
        raise ValueError("Side not understood.")