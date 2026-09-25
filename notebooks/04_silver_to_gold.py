"""Stage 04 -- Silver -> Gold (dimensional model).

Builds the star schema that the Direct Lake semantic model sits on. Dimensions are
written before the fact so that a referential-integrity check in stage 03 has
something to validate against.

Orchestrated by: Data Factory activity ``Silver_To_Gold``.

Parameters:
    env, pipeline, run_id, optimize

TODO(stage-04): add ``fact_risk_score`` once the R1-R5 risk rules land in
``cas_audit.risk.rules``. Keeping risk in its own fact table means the scoring
methodology can be re-run and re-versioned without rebuilding fact_payment.
"""

# ---------------------------------------------------------------- PARAMETERS
env = "local"
pipeline = "gold_star_schema"
run_id = ""
optimize = True
# ------------------------------------------------------------ END PARAMETERS

from cas_audit import config as cfg, io
from cas_audit.runtime import exit_notebook, get_logger, new_run_id
from cas_audit.session import get_spark
from cas_audit.transforms import gold

log = get_logger("stage04.gold")


def main() -> dict:
    rid = run_id or new_run_id()
    env_cfg, pipe_cfg = cfg.load(pipeline, env)
    spark = get_spark(env_cfg, app_name=f"gold-{pipeline}")

    source_table = pipe_cfg.source["table"]
    silver_path = env_cfg.path("silver", source_table)
    if not io.table_exists(spark, silver_path):
        raise FileNotFoundError(f"Silver table missing: {silver_path}. Run stage 02 first.")

    silver_df = io.read_delta(spark, silver_path)
    # Every builder scans this; caching once avoids five reads of the same table.
    silver_df.cache()
    log.info("run_id=%s silver.%s rows=%s", rid, source_table, silver_df.count())

    written: dict[str, int] = {}
    try:
        # Dimensions first, fact last -- the fact's keys must have parents to point at.
        for entity in pipe_cfg.get("entities", []):
            name = entity["name"]
            builder = gold.BUILDERS.get(name)
            if builder is None:
                raise ValueError(f"No builder registered for gold entity '{name}'")

            df = builder(silver_df)
            target = env_cfg.path("gold", name)
            partition_by = [c for c in entity.get("partition_by", []) if c in df.columns]
            io.write_delta(
                df,
                path=target,
                mode=entity.get("mode", "overwrite"),
                partition_by=partition_by,
            )

            written[name] = io.read_delta(spark, target).count()
            log.info("gold.%-18s rows=%-10s partitions=%s", name, written[name], partition_by or "-")

            # V-Order + compaction is what keeps Direct Lake resident in memory
            # rather than falling back to DirectQuery on first query.
            if optimize and env_cfg.env == "fabric":
                spark.sql(f"OPTIMIZE delta.`{target}`")
                log.info("optimised gold.%s", name)
    finally:
        silver_df.unpersist()

    result = {
        "stage": "04_gold",
        "run_id": rid,
        "entities_written": written,
        "fact_rows": written.get("fact_payment", 0),
        "optimized": bool(optimize and env_cfg.env == "fabric"),
    }
    log.info("gold complete: %s", written)
    return result


if __name__ == "__main__":
    exit_notebook(main())
