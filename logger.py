import logging


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='[%(name)s] %(message)s')
