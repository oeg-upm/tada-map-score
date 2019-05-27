import logging


def get_logger(name, level=logging.INFO):
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=level)
    logger = logging.getLogger(name)
    return logger
