class UserError(Exception):
    pass


class UserAuthError(UserError):
    pass


class RateLimitError(UserError):
    pass


class QuotaError(UserError):
    pass


class ProviderError(Exception):
    pass


recognized_errors = [UserError, UserAuthError, RateLimitError, QuotaError, ProviderError]


def is_internal_error(e: Exception) -> bool:
    return any(isinstance(e, recognized_error) for recognized_error in recognized_errors)
