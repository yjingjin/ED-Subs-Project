"""Spark session helper.

On Databricks a ``spark`` session already exists; this returns the active one. Locally
(PyCharm) it builds a lightweight session so the same transform code can be unit-tested.
"""

from __future__ import annotations


def get_spark(app_name: str = "ed_subs"):
    """Return the active SparkSession, creating a local one if needed."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.appName(app_name).getOrCreate()
