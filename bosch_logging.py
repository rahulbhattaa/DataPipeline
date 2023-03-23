import logging
import sys

def init_logging():
    log_formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    handler_out = logging.StreamHandler(sys.stdout)
    handler_out.setLevel(logging.DEBUG)
    handler_out.addFilter(lambda log: 1 if log.levelno < logging.WARNING else 0)
    handler_out.setFormatter(log_formatter)

    handler_err = logging.StreamHandler(sys.stderr)
    handler_err.setLevel(logging.WARNING)
    handler_err.setFormatter(log_formatter)    

    logging.basicConfig(level=logging.INFO, handlers=[handler_out, handler_err])