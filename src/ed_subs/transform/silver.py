"""Bronze -> Silver transformations.

Silver tables are cleaned, typed, deduplicated, and have conformed keys. Concrete
column casts and dedup keys are filled in once the source schemas are confirmed.
"""

from __future__ import annotations

from ed_subs.config import Config
from ed_subs.utils.spark import get_spark


def build_silver(table_name: str, config: Config) -> str:
    """Build a silver table from its bronze counterpart.

    Placeholder pass-through until source schemas are defined. Extend with:
    typed casts, null handling, deduplication, and key conformance.
    """
    spark = get_spark()
    source = config.table("bronze", table_name)
    target = config.table("silver", table_name)

    df = spark.table(source)

    # TODO: cast types, trim/clean strings, dedupe on business key, conform keys.

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target)
    return target
