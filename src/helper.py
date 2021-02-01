import logging
import time
from functools import wraps

from google.cloud import language_v1

CLIENT = language_v1.LanguageServiceClient()

TYPE_ = language_v1.Document.Type.PLAIN_TEXT
ENCODING_TYPE = language_v1.EncodingType.UTF8

logging.basicConfig(level=logging.INFO)


def retry(func=None, exception=Exception, n_tries=5, delay=10, backoff=2, logger=False):
    """Retry decorator
    Args:
        func: Function which needs to be decorated
        exception: List of exception to handle
        n_tries: Number of tries
        delay: Initial delay
        backoff: Backoff multiplication factor
        logger: Whether to log the error

    Returns:
        Decorated function

    """

    @wraps(func)
    def inner_wrapper(*args, **kwargs):
        ntries, ndelay = n_tries, delay

        while ntries > 1:
            try:
                return func(*args, **kwargs)
            except exception as e:
                err_message = f"{str(e)}, Retrying in {ndelay} seconds..."
                if logger:
                    logging.warning(err_message)
                else:
                    print(err_message)
                time.sleep(ndelay)
                ntries -= 1
                ndelay *= backoff

        return func(*args, **kwargs)

    return inner_wrapper
