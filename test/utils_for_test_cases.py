# Utilities for generating test scripts


from random import randint


def gen_id(length):
    s = ""
    for i in range(length):
        s += str(randint(0, 9))
    return s