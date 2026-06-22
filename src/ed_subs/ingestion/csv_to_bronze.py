"""Load a CSV (uploaded to the scratch catalog) into a bronze Delta table, 1:1 with source.

Bronze = raw landing. Minimal transformation: read as-is, add ingestion metadata, persist.
Typing/cleaning happens in the silver layer.
"""

from __future__ import annotations

from ed_subs.config import Config
from ed_subs.utils.spark import get_spark


def csv_to_bronze(csv_path: str, table_name: str, config: Config) -> str:
    """Read ``csv_path`` and write it to ``<catalog>.<bronze>.<table_name>`` as Delta.

    Args:
        csv_path: Path/volume location of the uploaded CSV in Databricks.
        table_name: Target bronze table name (typically matches the source table).
        config: Resolved pipeline config.

    Returns:
        The fully-qualified bronze table name that was written.
    """
    from pyspark.sql import functions as F

    spark = get_spark()
    target = config.table("bronze", table_name)

    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)  # keep raw strings; type in silver
        .csv(csv_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(csv_path))
    )

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target)
    return target
