"""Stage 01 -- Files (landing) -> Bronze Delta.

Converts the landed CSV extracts to partitioned Delta tables with lineage
columns attached. Bronze is a faithful, typed, queryable mirror of source: no
business rules, no filtering, no renaming. Corrupt rows are captured rather than
dropped, because "we silently discarded 4,000 records" is itself an audit finding.

Orchestrated by: Data Factory activity ``Files_To_Bronze`` (per-dataset, driven by
a ForEach over the manifest datasets).

Parameters:
    env, pipeline, run_id, manifest_path

TODO(stage-01): supply explicit schemas in config/schemas/<dataset>.json. Until
then columns land as strings, which is the correct Bronze default -- typing is a
Silver concern.
"""

# ---------------------------------------------------------------- PARAMETERS
env = "local"
pipeline = "bronze_general_payments"
run_id = ""
manifest_path = ""
# ------------------------------------------------------------ END PARAMETERS

import json
import os

from pyspark.sql import functions as F

from cas_audit import config as cfg, io
from cas_audit.runtime import exit_notebook, get_logger, new_run_id
from cas_audit.session import get_spark

log = get_logger("stage01.bronze")


def resolve_inputs(env_cfg: cfg.EnvConfig, pipe_cfg: cfg.PipelineConfig,
                   manifest_path: str) -> list[str]:
    """Prefer the manifest from stage 00; fall back to the configured glob.

    Reading the manifest means Bronze only ever processes files that stage 00
    accepted and fingerprinted.
    """
    dataset = pipe_cfg.source.get("dataset")
    if manifest_path and os.path.exists(manifest_path):
        with open(manifest_path, encoding="utf-8") as fh:
            entries = json.load(fh)
        paths = [e["landed_path"] for e in entries if e["dataset"] == dataset]
        if paths:
            log.info("manifest supplied %d file(s) for dataset=%s", len(paths), dataset)
            return paths
        log.warning("manifest had no entries for dataset=%s; falling back to glob", dataset)
    pattern = pipe_cfg.source["pattern"]
    return [env_cfg.path("files", pattern)]


def load_schema(pipe_cfg: cfg.PipelineConfig):
    """Load an explicit Spark schema if one is configured.

    Explicit schemas matter here: inferSchema on a 9 GB CSV costs a full extra
    pass over the data, and lets column types drift between program years.
    """
    rel = pipe_cfg.source.get("schema")
    if not rel:
        return None
    from pyspark.sql.types import StructType
    path = cfg.CONFIG_ROOT / rel
    if not path.exists():
        log.warning("configured schema %s not found; reading as strings", rel)
        return None
    with path.open(encoding="utf-8") as fh:
        return StructType.fromJson(json.load(fh))


def main() -> dict:
    rid = run_id or new_run_id()
    env_cfg, pipe_cfg = cfg.load(pipeline, env)
    spark = get_spark(env_cfg, app_name=f"bronze-{pipeline}")

    inputs = resolve_inputs(env_cfg, pipe_cfg, manifest_path)
    target = env_cfg.path("bronze", pipe_cfg.target["table"])
    log.info("run_id=%s reading %d input path(s) -> %s", rid, len(inputs), target)

    df = io.read_csv(
        spark,
        path=inputs if len(inputs) > 1 else inputs[0],
        options=pipe_cfg.source.get("options"),
        schema=load_schema(pipe_cfg),
    )

    # Separate corrupt rows instead of letting PERMISSIVE mode hide them.
    quarantined = 0
    if "_corrupt_record" in df.columns:
        bad = df.filter(F.col("_corrupt_record").isNotNull())
        quarantined = bad.count()
        if quarantined:
            qpath = env_cfg.path("quarantine", pipe_cfg.target["table"], rid)
            bad.write.mode("overwrite").json(qpath)
            log.warning("quarantined %d unparseable row(s) -> %s", quarantined, qpath)
        df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    df = io.with_audit_columns(df, run_id=rid)

    # Local runs work a peer-manufacturer subset; Fabric runs the full population.
    mfr_col = pipe_cfg.source.get("manufacturer_column")
    if mfr_col and mfr_col in df.columns:
        df = io.apply_sampling(df, env_cfg, mfr_col)

    partition_by = [c for c in pipe_cfg.target.get("partition_by", []) if c in df.columns]
    io.write_delta(
        df,
        path=target,
        mode=pipe_cfg.target.get("mode", "overwrite"),
        partition_by=partition_by,
    )

    rows = io.read_delta(spark, target).count()
    result = {
        "stage": "01_bronze",
        "run_id": rid,
        "pipeline": pipeline,
        "table": pipe_cfg.target["table"],
        "target_path": target,
        "rows_written": rows,
        "rows_quarantined": quarantined,
        "partitioned_by": partition_by,
        "sampled": env_cfg.sampling_enabled,
    }
    log.info("bronze.%s rows=%s quarantined=%s", pipe_cfg.target["table"], rows, quarantined)
    return result


if __name__ == "__main__":
    exit_notebook(main())
