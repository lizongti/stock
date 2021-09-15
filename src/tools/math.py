def is_int(s):
    try:
        n = int(s)
        return isinstance(n, int)
    except ValueError:
        return False
