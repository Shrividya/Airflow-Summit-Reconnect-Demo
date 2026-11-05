from __future__ import annotations


class RateLimited(ConnectionError):
    pass


class Unauthorized(PermissionError):
    pass


class SchemaDrift(ValueError):
    pass


class WarehouseSuspended(RuntimeError):
    pass


class MalformedPayload(ValueError):
    pass
