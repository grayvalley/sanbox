from enum import Enum


class SIDE(Enum):
    B = 1
    S = 2


def side_to_str(side):
    if side == SIDE.B:
        return 'B'
    elif side == SIDE.S:
        return 'S'
    else:
        raise ValueError("Side not understood.")


def get_opposite_side(side):
    if side == SIDE.B:
        return SIDE.S
    elif side == SIDE.S:
        return SIDE.B
    else:
        raise ValueError('Side has to be either Side.B or Side.S.')
