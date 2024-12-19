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
