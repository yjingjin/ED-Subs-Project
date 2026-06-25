"""Configuration loading for the ED Subs pipeline.

Reads ``conf/config.yaml`` and allows environment-variable overrides so the same code
runs locally (PyCharm) and on Databricks. Keeps catalog/schema/prefix names in one place.

All medallion tables live in a single schema (cannot create new schemas in the scratch
catalog). Layers are distinguished by table name prefixes:
    ed_bronze_<table>, ed_silver_<table>, ed_gold_<table>
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CONFIG_PATH = _REPO_ROOT / "conf" / "config.yaml"

_DEFAULT_CATALOG = "general_scratch_catalog"
_DEFAULT_SCHEMA  = "general_scratch"
_DEFAULT_VOLUME  = (
    "/Volumes/general_scratch_catalog/general_scratch"
    "/checkpoints/jiny/ed_subs_raw_uploads"
)


@dataclass
class Config:
    """Resolved pipeline configuration."""

    catalog: str
    schema: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    export_dir: Path
    volume_upload_dir: str
    raw: dict[str, Any] = field(default_factory=dict)

    def table(self, layer: str, name: str) -> str:
        """Return a fully-qualified table name.

        Example:
            cfg.table("silver", "subscriptions")
            # → "general_scratch_catalog.general_scratch.ed_silver_subscriptions"
        """
        prefix = {
            "bronze": self.bronze_prefix,
            "silver": self.silver_prefix,
            "gold":   self.gold_prefix,
        }[layer]
        return f"{self.catalog}.{self.schema}.{prefix}{name}"


def load_config(path: str | Path | None = None) -> Config:
    """Load config from YAML, applying ``ED_*`` environment overrides where present."""
    path = Path(path) if path else _DEFAULT_CONFIG_PATH
    data: dict[str, Any] = {}
    if path.exists():
        data = yaml.safe_load(path.read_text()) or {}

    prefixes = data.get("table_prefixes", {})
    paths    = data.get("paths", {})

    return Config(
        catalog=os.getenv("ED_CATALOG", data.get("catalog", _DEFAULT_CATALOG)),
        schema=os.getenv("ED_SCHEMA",   data.get("schema",  _DEFAULT_SCHEMA)),
        bronze_prefix=os.getenv("ED_BRONZE_PREFIX", prefixes.get("bronze", "ed_bronze_")),
        silver_prefix=os.getenv("ED_SILVER_PREFIX", prefixes.get("silver", "ed_silver_")),
        gold_prefix=os.getenv(  "ED_GOLD_PREFIX",   prefixes.get("gold",   "ed_gold_")),
        export_dir=Path(os.getenv("ED_EXPORT_DIR", paths.get("export_dir", "data/exports"))),
        volume_upload_dir=os.getenv(
            "ED_VOLUME_UPLOAD_DIR", paths.get("volume_upload_dir", _DEFAULT_VOLUME)
        ),
        raw=data,
    )
