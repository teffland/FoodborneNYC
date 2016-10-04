""" Loggin Utlilities """
import logging
import logging.handlers
import sys

from haystack.config import config

def get_logger(appname=None, level='WARNING'):
    """ Provides a system-level logger, accessible from any module.

        Correct function signature is get_logger(__name__).
        This will log from whatever module it was called in.

        If appname is None, the root logger will be returned.
    """
    logger = logging.getLogger(appname)
    logger.setLevel(level)
    format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # log to std out
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(format)
    logger.addHandler(stream_handler)
    # log to the configured logfile dir
    file_handler = logging.handlers.RotatingFileHandler(
                        '{}/{}.log'.format(config['logging_dir'], 'app'),
                        maxBytes=1e9,
                        backupCount=5)
    file_handler.setFormatter(format)
    logger.addHandler(file_handler)
    return logger
