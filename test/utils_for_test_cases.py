# Utilities for generating test scripts


from random import randint


def gen_numerical_id(length):
    s = ""
    for i in range(length):
        s += str(randint(0, 9))
    return s


def gen_messenger_id(length):
    s = ""
    chars = "abcdefghijklmnopqrstuvwxyz0123456789-_"
    for i in range(length):
        s += chars[randint(0, len(chars))]
    return s
