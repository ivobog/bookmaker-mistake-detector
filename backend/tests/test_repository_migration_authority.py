from __future__ import annotations

from bookmaker_detector_api.repositories.postgres_migration_authority import (
    get_postgres_migration_authority,
    list_bootstrap_sql_chain,
)


def test_bootstrap_sql_chain_is_ordered_and_present() -> None:
    chain = list_bootstrap_sql_chain()

    assert [entry.order for entry in chain] == list(range(1, len(chain) + 1))
    assert all(entry.abspath.exists() for entry in chain)


def test_postgres_migration_authority_is_explicitly_deferred() -> None:
    decision = get_postgres_migration_authority()

    assert decision.current_authority == "bootstrap_sql_chain"
    assert decision.alembic_status == "deferred"
    assert len(decision.rationale) >= 3
    assert len(decision.introduction_triggers) >= 3
