import time
from random import randint


def sleep_condition(range1, range2):
    """
    This function is used to define wait time based on two different range seconds

    Params:
    range1 (int) | Required : Start range number to set wait time
    range2 (int) | Required : End range number to set wait time

    Example:
    sleep_condition(5, 10)
    """
    sleep_time = randint(range1, range2)
    return time.sleep(sleep_time)
