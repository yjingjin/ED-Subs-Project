"""Configuration loading for the ED Subs pipeline.

Reads ``conf/config.yaml`` and allows environment-variable overrides so the same code
runs locally (PyCharm) and on Databricks. Keeps schema/catalog names in one place.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CONFIG_PATH = _REPO_ROOT / "conf" / "config.yaml"


@dataclass
class Config:
    """Resolved pipeline configuration."""

    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    export_dir: Path
    raw: dict[str, Any] = field(default_factory=dict)

    def table(self, layer: str, name: str) -> str:
        """Return a fully-qualified table name, e.g. ``scratch.ed_silver.subscriptions``."""
        schema = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
        }[layer]
        return f"{self.catalog}.{schema}.{name}"


def load_config(path: str | Path | None = None) -> Config:
    """Load config from YAML, applying ``ED_*`` environment overrides where present."""
    path = Path(path) if path else _DEFAULT_CONFIG_PATH
    data: dict[str, Any] = {}
    if path.exists():
        data = yaml.safe_load(path.read_text()) or {}

    schemas = data.get("schemas", {})
    paths = data.get("paths", {})

    return Config(
        catalog=os.getenv("ED_CATALOG", data.get("catalog", "scratch")),
        bronze_schema=os.getenv("ED_BRONZE_SCHEMA", schemas.get("bronze", "ed_bronze")),
        silver_schema=os.getenv("ED_SILVER_SCHEMA", schemas.get("silver", "ed_silver")),
        gold_schema=os.getenv("ED_GOLD_SCHEMA", schemas.get("gold", "ed_gold")),
        export_dir=Path(os.getenv("ED_EXPORT_DIR", paths.get("export_dir", "data/exports"))),
        raw=data,
    )
