"""Stage 02 -- Bronze -> Silver (ELT).

Applies typing, cleansing, conforming and deduplication to produce the trusted
Silver payment grain. General and Research payments are unioned onto one grain so
downstream risk rules are written once.

Orchestrated by: Data Factory activity ``Bronze_To_Silver``.

Parameters:
    env, pipeline, run_id

TODO(stage-02): add incremental (MERGE) mode once CMS publishes mid-year
restatements we need to apply without a full reload. Today this is a full refresh,
which is correct for an annual publication cycle.
"""

# ---------------------------------------------------------------- PARAMETERS
env = "local"
pipeline = "silver_payments"
run_id = ""
# ------------------------------------------------------------ END PARAMETERS

from functools import reduce

from pyspark.sql import DataFrame

from cas_audit import config as cfg, io
from cas_audit.runtime import exit_notebook, get_logger, new_run_id
from cas_audit.session import get_spark
from cas_audit.transforms import silver

log = get_logger("stage02.silver")

# Which Bronze table maps through which conforming function.
CONFORMERS = {
    "GENERAL": silver.conform_general_payments,
    "RESEARCH": silver.conform_research_payments,
}


def main() -> dict:
    rid = run_id or new_run_id()
    env_cfg, pipe_cfg = cfg.load(pipeline, env)
    spark = get_spark(env_cfg, app_name=f"silver-{pipeline}")

    sources = pipe_cfg.source.get("tables", [])
    if not sources:
        raise ValueError(f"{pipeline}: source.tables is empty")

    frames: list[DataFrame] = []
    source_counts: dict[str, int] = {}

    for spec in sources:
        table, category = spec["table"], spec["category"]
        path = env_cfg.path("bronze", table)
        if not io.table_exists(spark, path):
            log.warning("skipping absent bronze table %s", path)
            continue
        bronze_df = io.read_delta(spark, path)
        source_counts[table] = bronze_df.count()
        conformed = CONFORMERS[category](bronze_df)
        frames.append(conformed)
        log.info("conformed %s (%s) rows=%s", table, category, source_counts[table])

    if not frames:
        raise RuntimeError("No bronze inputs available; run stage 01 first.")

    # unionByName tolerates column-order differences between the two extracts.
    unioned = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    silver_df = silver.build_silver_payments(unioned)

    target = env_cfg.path("silver", pipe_cfg.target["table"])
    partition_by = [c for c in pipe_cfg.target.get("partition_by", []) if c in silver_df.columns]
    io.write_delta(
        silver_df,
        path=target,
        mode=pipe_cfg.target.get("mode", "overwrite"),
        partition_by=partition_by,
    )

    rows_out = io.read_delta(spark, target).count()
    rows_in = sum(source_counts.values())
    result = {
        "stage": "02_silver",
        "run_id": rid,
        "table": pipe_cfg.target["table"],
        "target_path": target,
        "rows_in": rows_in,
        "rows_out": rows_out,
        # Reconciliation figure: the drop must be explainable by dedupe + DELETEs.
        "rows_removed": rows_in - rows_out,
        "source_counts": source_counts,
    }
    log.info("silver.%s rows_in=%s rows_out=%s removed=%s",
             pipe_cfg.target["table"], rows_in, rows_out, result["rows_removed"])
    return result


if __name__ == "__main__":
    exit_notebook(main())
