from dataclasses import dataclass
from typing import Optional


@dataclass
class RetryStrategyConfig:
    """
    Contains all info needed for decorator to pull from `self` for backoff
    and retry triggered by exception.

    Args:
        max_retries: The maximum number of attempts to make before giving
            up. Once exhausted, the exception will be allowed to escape.
            The default value of None means there is no limit to the
            number of tries. If a callable is passed, it will be
            evaluated at runtime and its return value used.
        max_retry_time: The maximum total amount of time to try for before
            giving up. Once expired, the exception will be allowed to
            escape. If a callable is passed, it will be
            evaluated at runtime and its return value used.
    """

    max_retries: Optional[int] = None
    max_retry_time: Optional[float] = None
