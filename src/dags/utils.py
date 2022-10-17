import requests
import time
import logging
from collections.abc import Callable

# UTILS
def simple_retry(request: Callable, kwargs: dict) -> requests.Response:
    """Простая функция повтора до 3 раз запроса.

    Args:
        request: Функция запроса get/post
        kwargs: Аргументы вызова функции request

    Returns:
        requests.Response

    """
    response = None
    for _ in range(3):
        try:
            response = request(**kwargs)
            if response.status_code == requests.codes.ok:
                raise requests.exceptions.HTTPError()
        except:
            logging.info("Bad response. Sleep 10sec to reply.")
            time.sleep(10)
            continue
    return response