"""Read/write helpers shared by every layer.

All table access goes through here so that audit lineage columns, write options
and Delta conventions are applied once rather than copy-pasted into five
notebooks.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import EnvConfig

# Lineage columns stamped onto every Bronze record. CAS needs to answer
# "where did this row come from and when did we load it" for any finding.
INGEST_TIMESTAMP = "_ingest_timestamp"
INGEST_SOURCE_FILE = "_ingest_source_file"
INGEST_RUN_ID = "_ingest_run_id"
AUDIT_COLUMNS = [INGEST_TIMESTAMP, INGEST_SOURCE_FILE, INGEST_RUN_ID]


def read_csv(spark: SparkSession, path: str, options: dict[str, Any] | None = None,
             schema=None) -> DataFrame:
    """Read delimited source files.

    Open Payments embeds commas inside quoted fields (``"Lilly USA, LLC"``), so
    quote/escape handling is mandatory -- a naive split corrupts the manufacturer
    dimension.
    """
    defaults = {
        "header": "true",
        "quote": '"',
        "escape": '"',
        "multiLine": "true",
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": "_corrupt_record",
    }
    defaults.update({k: str(v) for k, v in (options or {}).items()})
    reader = spark.read.options(**defaults)
    if schema is not None:
        reader = reader.schema(schema)
    else:
        # Only safe on small files -- inferSchema costs a full extra pass.
        reader = reader.option("inferSchema", "false")
    return reader.csv(path)


def with_audit_columns(df: DataFrame, run_id: str) -> DataFrame:
    """Stamp lineage columns used for traceability from Gold back to source."""
    return (
        df.withColumn(INGEST_TIMESTAMP, F.current_timestamp())
        .withColumn(INGEST_SOURCE_FILE, F.input_file_name())
        .withColumn(INGEST_RUN_ID, F.lit(run_id))
    )


def write_delta(df: DataFrame, path: str, mode: str = "overwrite",
                partition_by: list[str] | None = None,
                merge_schema: bool = False) -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.save(path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def table_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False


def apply_sampling(df: DataFrame, env: EnvConfig, manufacturer_col: str) -> DataFrame:
    """Restrict local runs to peer manufacturers so iteration stays fast.

    Matching is case-insensitive on a trimmed name. CMS submitters report their own
    entity name with no casing convention -- ``PFIZER INC.`` sits alongside
    ``AstraZeneca Pharmaceuticals LP`` in the same extract -- so an exact-match
    filter silently drops entities that are present.

    Fabric disables sampling and processes the full population; the assurance claim
    ("we tested 100%, not a sample") depends on that.
    """
    if not env.sampling_enabled:
        return df
    names = env.sampled_manufacturers
    if not names:
        return df
    wanted = [n.strip().upper() for n in names]
    return df.filter(F.upper(F.trim(F.col(manufacturer_col))).isin(wanted))
