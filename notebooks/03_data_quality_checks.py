"""Stage 03 -- Data quality gate.

Evaluates the expectations declared in a pipeline config against a materialised
table, writes the outcome to the ops results table, and fails the run when an
error-severity expectation breaks.

This notebook is deliberately generic: Data Factory calls the same activity after
Bronze, after Silver and after Gold, passing a different ``pipeline`` each time.
One tested gate beats three near-identical copies.

Duplicate detection is the headline check. For Open Payments the business key is
``payment_id`` (CMS ``Record_ID``); a surplus there means either a CMS restatement
we failed to collapse in Silver, or a double-load -- both of which would overstate
total transfers of value and invalidate any conclusion drawn from the population.

Orchestrated by: Data Factory activity ``Data_Quality_Checks``.

Parameters:
    env, pipeline, layer, table, run_id, fail_on_error
"""

# ---------------------------------------------------------------- PARAMETERS
env = "local"
pipeline = "silver_payments"
layer = "silver"
table = ""          # defaults to the pipeline's target table
run_id = ""
fail_on_error = True
# ------------------------------------------------------------ END PARAMETERS

from cas_audit import config as cfg, io
from cas_audit.quality import checks
from cas_audit.runtime import exit_notebook, get_logger, new_run_id
from cas_audit.session import get_spark

log = get_logger("stage03.dq")


def main() -> dict:
    rid = run_id or new_run_id()
    env_cfg, pipe_cfg = cfg.load(pipeline, env)
    spark = get_spark(env_cfg, app_name=f"dq-{pipeline}")

    target_table = table or pipe_cfg.target["table"]
    path = env_cfg.path(layer, target_table)

    if not io.table_exists(spark, path):
        raise FileNotFoundError(f"Cannot run DQ: {path} does not exist. Run the upstream stage first.")

    df = io.read_delta(spark, path)
    expectations = pipe_cfg.expectations
    if not expectations:
        log.warning("%s declares no expectations -- nothing to verify", pipeline)

    log.info("run_id=%s evaluating %d expectation(s) against %s.%s",
             rid, len(expectations), layer, target_table)

    # Cache: several checks scan the same DataFrame, and re-reading a large
    # Delta table per check is the difference between seconds and minutes.
    df.cache()
    try:
        results = checks.run_expectations(df, expectations, run_id=rid, pipeline=pipeline)
    finally:
        df.unpersist()

    for r in results:
        log.log(
            20 if r.passed else (40 if r.severity == "error" else 30),
            "%-7s %-16s %-28s %s",
            "PASS" if r.passed else "FAIL", r.check_type, r.columns, r.detail,
        )

    checks.persist_results(spark, results, env_cfg.path("dq_results"))

    failed = [r for r in results if not r.passed]
    result = {
        "stage": "03_data_quality",
        "run_id": rid,
        "pipeline": pipeline,
        "table": f"{layer}.{target_table}",
        "checks_total": len(results),
        "checks_passed": sum(1 for r in results if r.passed),
        "checks_failed": len(failed),
        "errors": [r.detail for r in failed if r.severity == "error"],
        "warnings": [r.detail for r in failed if r.severity == "warn"],
        "summary": checks.summarise(results),
    }
    log.info("%s -> %s", target_table, result["summary"])

    # Raise last, after results are persisted, so a failure is still evidenced.
    if fail_on_error:
        checks.enforce(results)
    return result


if __name__ == "__main__":
    exit_notebook(main())
