from healing.errors import RateLimited, Unauthorized


def test_rate_limited_is_a_connection_error():
    assert issubclass(RateLimited, ConnectionError)


def test_unauthorized_is_a_permission_error():
    assert issubclass(Unauthorized, PermissionError)
    assert not issubclass(Unauthorized, ConnectionError)
