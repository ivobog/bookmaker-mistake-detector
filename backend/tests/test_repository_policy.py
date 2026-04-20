from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from bookmaker_detector_api.config import Settings
from bookmaker_detector_api.repositories import IngestionRepository as package_ingestion_repository
from bookmaker_detector_api.repositories import (
    ingestion_postgres_repository as postgres_repo_module,
)
from bookmaker_detector_api.repositories.ingestion import (
    IngestionRepository as shim_ingestion_repository,
)
from bookmaker_detector_api.repositories.ingestion_postgres_repository import (
    PostgresIngestionRepository,
)
from bookmaker_detector_api.repositories.ingestion_types import (
    IngestionRepository as canonical_ingestion_repository,
)
from bookmaker_detector_api.services import repository_factory as repo_factory


def test_settings_resolved_postgres_allow_runtime_schema_mutation_defaults_to_disabled() -> None:
    assert (
        Settings(
            api_env="production",
            postgres_allow_runtime_schema_mutation=None,
        ).resolved_postgres_allow_runtime_schema_mutation
        is False
    )
    assert (
        Settings(
            api_env="development",
            postgres_allow_runtime_schema_mutation=None,
        ).resolved_postgres_allow_runtime_schema_mutation
        is False
    )
    assert (
        Settings(
            api_env="production",
            postgres_allow_runtime_schema_mutation=True,
        ).resolved_postgres_allow_runtime_schema_mutation
        is True
    )


def test_ingestion_repository_contract_exports_resolve_to_canonical_protocol() -> None:
    assert package_ingestion_repository is canonical_ingestion_repository
    assert shim_ingestion_repository is canonical_ingestion_repository


def test_build_bootstrap_postgres_ingestion_repository_enables_runtime_schema_mutation(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeRepository:
        def __init__(self, connection, *, allow_runtime_schema_mutation=True) -> None:
            captured["connection"] = connection
            captured["allow_runtime_schema_mutation"] = allow_runtime_schema_mutation

    monkeypatch.setattr(repo_factory, "PostgresIngestionRepository", FakeRepository)

    repository = repo_factory.build_bootstrap_postgres_ingestion_repository("bootstrap-connection")

    assert isinstance(repository, FakeRepository)
    assert captured == {
        "connection": "bootstrap-connection",
        "allow_runtime_schema_mutation": True,
    }


def test_build_postgres_ingestion_repository_passes_runtime_schema_policy(
    monkeypatch,
) -> None:
    @contextmanager
    def fake_postgres_connection():
        yield "fake-connection"

    captured: dict[str, object] = {}

    class FakeRepository:
        def __init__(self, connection, *, allow_runtime_schema_mutation=True) -> None:
            captured["connection"] = connection
            captured["allow_runtime_schema_mutation"] = allow_runtime_schema_mutation

    monkeypatch.setattr(repo_factory, "postgres_connection", fake_postgres_connection)
    monkeypatch.setattr(repo_factory, "PostgresIngestionRepository", FakeRepository)
    monkeypatch.setattr(
        repo_factory,
        "settings",
        SimpleNamespace(resolved_postgres_allow_runtime_schema_mutation=False),
    )

    repository, repository_context = repo_factory.build_postgres_ingestion_repository()

    assert isinstance(repository, FakeRepository)
    assert captured == {
        "connection": "fake-connection",
        "allow_runtime_schema_mutation": False,
    }
    assert repository_context is not None
    repository_context.__exit__(None, None, None)


def test_build_bootstrap_postgres_ingestion_repository_supports_legacy_constructor(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        repo_factory,
        "PostgresIngestionRepository",
        lambda connection: {"connection": connection, "kind": "legacy"},
    )

    repository = repo_factory.build_bootstrap_postgres_ingestion_repository("legacy-connection")

    assert repository == {"connection": "legacy-connection", "kind": "legacy"}


@pytest.mark.parametrize(
    ("method_name", "ensure_name", "verify_name", "ready_flag"),
    [
        (
            "_ensure_raw_row_source_identity_schema",
            "ensure_raw_row_source_identity_schema",
            "verify_raw_row_source_identity_schema",
            "_raw_row_source_identity_ready",
        ),
        (
            "_ensure_data_quality_issue_identity_schema",
            "ensure_data_quality_issue_identity_schema",
            "verify_data_quality_issue_identity_schema",
            "_data_quality_issue_identity_ready",
        ),
    ],
)
def test_postgres_repository_schema_policy_raises_when_mutation_disabled_and_schema_missing(
    monkeypatch,
    method_name: str,
    ensure_name: str,
    verify_name: str,
    ready_flag: str,
) -> None:
    ensure_calls = 0
    verify_calls = 0

    def fake_ensure(connection) -> None:
        nonlocal ensure_calls
        ensure_calls += 1

    def fake_verify(connection) -> bool:
        nonlocal verify_calls
        verify_calls += 1
        return False

    monkeypatch.setattr(postgres_repo_module, ensure_name, fake_ensure)
    monkeypatch.setattr(postgres_repo_module, verify_name, fake_verify)

    repository = PostgresIngestionRepository(object(), allow_runtime_schema_mutation=False)

    with pytest.raises(RuntimeError, match="schema is not ready"):
        getattr(repository, method_name)()

    assert ensure_calls == 0
    assert verify_calls == 1
    assert getattr(repository, ready_flag) is False


@pytest.mark.parametrize(
    ("method_name", "ensure_name", "verify_name", "ready_flag"),
    [
        (
            "_ensure_raw_row_source_identity_schema",
            "ensure_raw_row_source_identity_schema",
            "verify_raw_row_source_identity_schema",
            "_raw_row_source_identity_ready",
        ),
        (
            "_ensure_data_quality_issue_identity_schema",
            "ensure_data_quality_issue_identity_schema",
            "verify_data_quality_issue_identity_schema",
            "_data_quality_issue_identity_ready",
        ),
    ],
)
def test_postgres_repository_schema_policy_verifies_without_mutation_when_schema_ready(
    monkeypatch,
    method_name: str,
    ensure_name: str,
    verify_name: str,
    ready_flag: str,
) -> None:
    ensure_calls = 0
    verify_calls = 0

    def fake_ensure(connection) -> None:
        nonlocal ensure_calls
        ensure_calls += 1

    def fake_verify(connection) -> bool:
        nonlocal verify_calls
        verify_calls += 1
        return True

    monkeypatch.setattr(postgres_repo_module, ensure_name, fake_ensure)
    monkeypatch.setattr(postgres_repo_module, verify_name, fake_verify)

    repository = PostgresIngestionRepository(object(), allow_runtime_schema_mutation=False)
    getattr(repository, method_name)()

    assert ensure_calls == 0
    assert verify_calls == 1
    assert getattr(repository, ready_flag) is True


@pytest.mark.parametrize(
    ("method_name", "ensure_name", "verify_name", "ready_flag"),
    [
        (
            "_ensure_raw_row_source_identity_schema",
            "ensure_raw_row_source_identity_schema",
            "verify_raw_row_source_identity_schema",
            "_raw_row_source_identity_ready",
        ),
        (
            "_ensure_data_quality_issue_identity_schema",
            "ensure_data_quality_issue_identity_schema",
            "verify_data_quality_issue_identity_schema",
            "_data_quality_issue_identity_ready",
        ),
    ],
)
def test_postgres_repository_schema_policy_mutates_when_explicitly_enabled(
    monkeypatch,
    method_name: str,
    ensure_name: str,
    verify_name: str,
    ready_flag: str,
) -> None:
    ensure_calls = 0

    def fake_ensure(connection) -> None:
        nonlocal ensure_calls
        ensure_calls += 1

    def fail_verify(connection) -> bool:
        raise AssertionError("verify helper should not run when runtime mutation is enabled")

    monkeypatch.setattr(postgres_repo_module, ensure_name, fake_ensure)
    monkeypatch.setattr(postgres_repo_module, verify_name, fail_verify)

    repository = PostgresIngestionRepository(object(), allow_runtime_schema_mutation=True)
    getattr(repository, method_name)()

    assert ensure_calls == 1
    assert getattr(repository, ready_flag) is True
