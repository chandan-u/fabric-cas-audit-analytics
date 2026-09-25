"""Spark session factory.

In Fabric the session already exists and must be reused -- creating one is both
wrong and slow. Locally we build a Delta-enabled session. Notebook code calls
``get_spark(env)`` and stays identical across both.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from .config import EnvConfig


def _pin_worker_python() -> None:
    """Force Spark's Python workers to use the interpreter running the driver.

    Spark otherwise launches workers via ``python3`` from PATH. When the driver is
    a virtualenv on a different minor version than the system interpreter, any
    stage touching a Python worker fails with PYTHON_VERSION_MISMATCH. Fabric
    manages this itself, so this applies to local runs only.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark(env: EnvConfig, app_name: str = "cas-audit-analytics") -> SparkSession:
    active = SparkSession.getActiveSession()

    if env.env == "fabric":
        if active is None:  # pragma: no cover - only reachable inside Fabric
            raise RuntimeError("Expected an active Spark session in Fabric.")
        spark = active
    elif active is not None:
        spark = active
    else:
        _pin_worker_python()
        from delta import configure_spark_with_delta_pip

        builder = SparkSession.builder.appName(app_name)
        if master := env.spark.get("master"):
            builder = builder.master(master)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

    for key, value in (env.spark.get("configs") or {}).items():
        try:
            spark.conf.set(key, str(value))
        except Exception:
            # Fabric locks some configs at pool level; a rejected hint is not fatal.
            pass

    if partitions := env.spark.get("shuffle_partitions"):
        spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

    spark.sparkContext.setLogLevel("ERROR")
    return spark
