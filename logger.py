import logging


def get_logger(name, level=logging.DEBUG):
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(name)
    return logger
