import logging, os, sys, json
from pythonjsonlogger import jsonlogger

def setup_json_logging(name: str):
    logger = logging.getLogger(name)
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    fmt = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
    handler.setFormatter(fmt)
    # avoid duplicate handlers
    logger.handlers = []
    logger.addHandler(handler)
    logger.propagate = False
    return logger
