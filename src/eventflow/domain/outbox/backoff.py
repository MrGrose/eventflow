import math


def backoff_seconds(attempts: int) -> int:
    return min(300, int(math.pow(2, max(attempts, 1))))
