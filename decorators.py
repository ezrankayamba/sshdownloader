from functools import wraps
import inspect
import time
import common_logging as logging

logger = logging.getLogger(__name__)


def get_client_ip(request):
    if not request:
        return 'basic-auth'
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def timer(*dec_args):
    """helper function to estimate view execution time"""

    def decorator(func):
        module = inspect.getmodule(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            duration = (time.time() - start) * 1000
            msg = '{}: {} took {:.2f}ms: Args: {}'.format(
                module.__name__, func.__name__, duration, dec_args)
            logger.debug(f'Timer: {msg}')
            return result
        return wrapper
    return decorator
