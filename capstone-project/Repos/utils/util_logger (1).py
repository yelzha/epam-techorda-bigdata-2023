import logging
import os

def init_logger(logger_name):
    file_path = '/tmp/zhastay_yeltay_log.log'

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    formatter=logging.Formatter('%(asctime)s↭%(name)s↭%(levelname)s↭%(message)s', datefmt='%d/%m/%Y %H:%M:%S')

    logFileHandler = logging.FileHandler(file_path, mode='a') #a will append
    logFileHandler.setFormatter(formatter)
    logger.addHandler(logFileHandler)
    if len(logger.handlers) > 1:
        # remove the first handler
        logger.removeHandler(logger.handlers[0])

    return logger