"""Sanity tests for config loading (no Spark required)."""

from ed_subs.config import load_config


def test_load_config_defaults():
    cfg = load_config()
    assert cfg.catalog
    assert cfg.bronze_schema
    assert cfg.silver_schema
    assert cfg.gold_schema


def test_fully_qualified_table_name():
    cfg = load_config()
    fqn = cfg.table("silver", "subscriptions")
    assert fqn == f"{cfg.catalog}.{cfg.silver_schema}.subscriptions"


def test_env_override(monkeypatch):
    monkeypatch.setenv("ED_CATALOG", "my_scratch")
    cfg = load_config()
    assert cfg.catalog == "my_scratch"
