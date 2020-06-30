from yaml import load, CLoader

__config = None


def config():
    global __config
    if not __config:
        with open('config.yaml', mode='r') as f:
            __config = load(f, CLoader)
    return __config
