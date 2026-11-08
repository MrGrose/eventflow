class RetryableProcessingError(Exception):
    pass


class PermanentProcessingError(Exception):
    pass


def classify_error(exc: Exception) -> type[Exception]:
    if isinstance(exc, (RetryableProcessingError, TimeoutError, ConnectionError, OSError)):
        return RetryableProcessingError
    return PermanentProcessingError
