from yaml import safe_load

__config = None


def config():
    global __config
    if not __config:
        with open('config.yaml', mode='r') as f:
            __config = safe_load(f)
    return __config
