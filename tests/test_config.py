"""Sanity tests for config loading (no Spark required)."""

from ed_subs.config import load_config


def test_load_config_defaults():
    cfg = load_config()
    assert cfg.catalog == "general_scratch_catalog"
    assert cfg.schema  == "general_scratch"
    assert cfg.bronze_prefix
    assert cfg.silver_prefix
    assert cfg.gold_prefix


def test_fully_qualified_table_name():
    cfg = load_config()
    fqn = cfg.table("silver", "subscriptions")
    assert fqn == f"{cfg.catalog}.{cfg.schema}.{cfg.silver_prefix}subscriptions"


def test_all_layers():
    cfg = load_config()
    assert cfg.table("bronze", "subscriptions").endswith("ed_bronze_subscriptions")
    assert cfg.table("silver", "subscriptions").endswith("ed_silver_subscriptions")
    assert cfg.table("gold",   "subscriptions").endswith("ed_gold_subscriptions")


def test_env_override(monkeypatch):
    monkeypatch.setenv("ED_CATALOG", "my_catalog")
    monkeypatch.setenv("ED_SCHEMA",  "my_schema")
    cfg = load_config()
    assert cfg.catalog == "my_catalog"
    assert cfg.schema  == "my_schema"
