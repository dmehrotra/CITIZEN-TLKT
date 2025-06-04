import time
from functools import wraps
import logging

logger = logging.getLogger(__name__)


def timeit(method):
    @wraps(method)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = method(*args, **kwargs)
        end_time = time.time()
        logger.debug(f"{method.__name__} => {(end_time-start_time):0.5f} seconds")
        return result

    return wrapper
