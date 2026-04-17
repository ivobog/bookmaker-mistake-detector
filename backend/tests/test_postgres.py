from bookmaker_detector_api.db import postgres as postgres_module


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def execute(self, query, params):
        self.executed.append((query, params))

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, rows):
        self.cursor_calls = 0
        self.cursor_instance = _FakeCursor(rows)

    def cursor(self):
        self.cursor_calls += 1
        return self.cursor_instance


def test_ensure_required_postgres_schema_raises_clear_error_for_missing_tables() -> None:
    postgres_module.reset_postgres_schema_verification_cache()
    connection = _FakeConnection([("feature_version",), ("model_registry",)])

    try:
        postgres_module.ensure_required_postgres_schema(connection)
    except RuntimeError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected schema readiness check to fail")

    assert "PostgreSQL schema is not ready" in message
    assert "feature_version" in message
    assert "model_registry" in message
    assert "infra/postgres/init" in message
    assert connection.cursor_calls == 1


def test_ensure_required_postgres_schema_caches_successful_verification() -> None:
    postgres_module.reset_postgres_schema_verification_cache()
    connection = _FakeConnection([])

    postgres_module.ensure_required_postgres_schema(connection)
    postgres_module.ensure_required_postgres_schema(connection)

    assert connection.cursor_calls == 1
    executed_query, executed_params = connection.cursor_instance.executed[0]
    assert "to_regclass" in executed_query
    assert list(executed_params[0]) == list(postgres_module.REQUIRED_POSTGRES_TABLES)

    postgres_module.reset_postgres_schema_verification_cache()
