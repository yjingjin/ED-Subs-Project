"""Phase 1 (ingestion): load exported CSVs into the bronze layer.

Source CSVs are produced in DataGrip from Redshift and uploaded to the Databricks scratch
catalog. This package reads them and writes 1:1 Delta tables into ``ed_bronze``.
"""
