import re

def normalize(name):
    return re.sub('[^a-z ]', '', name.lower())


def name_grosser(norm_name):
    res = dict()
    for seg in norm_name.split(" "):
        if seg in res:
            res[seg] = res[seg] + 1
        else:
            res[seg] = 1
    return res


def flip(name_with_two_parts):
    first,last = name_with_two_parts.split(" ")
    return last + " " + first
