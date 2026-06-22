"""Silver -> Gold transformations.

Gold tables are model-ready: joined/aggregated feature tables plus the churn label.
The churn label definition is TBD and will be implemented in ``ed_subs.features``.
"""

from __future__ import annotations

from ed_subs.config import Config
from ed_subs.utils.spark import get_spark


def build_gold(table_name: str, config: Config) -> str:
    """Build a model-ready gold table from silver inputs.

    Placeholder until feature definitions and the churn label are specified.
    """
    spark = get_spark()
    target = config.table("gold", table_name)

    # TODO: join silver tables, engineer features, attach churn label.
    df = spark.table(config.table("silver", table_name))

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target)
    return target
