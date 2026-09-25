"""Gold-layer transforms: dimensional model for the Direct Lake semantic model.

A star schema, not a wide flat table. Direct Lake loads Delta column segments into
memory on demand, and a star with narrow dimensions plus integer-keyed facts is
what keeps it in Direct Lake mode instead of falling back to DirectQuery.

Grain:
    fact_payment      one row per Silver payment record
    dim_recipient     one row per covered recipient (physician or teaching hospital)
    dim_manufacturer  one row per reporting entity
    dim_nature        one row per nature-of-payment category
    dim_date          one row per calendar date in the payment range
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Surrogate-key convention: a deterministic hash of the business key.
# Deterministic keys mean a rebuilt Gold layer keeps the same keys, so a Power BI
# bookmark or a cited workpaper reference does not silently point somewhere else.
SK_BITS = 256


def _surrogate_key(*cols: str) -> "F.Column":
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in cols]), SK_BITS)


def build_dim_recipient(silver: DataFrame) -> DataFrame:
    """One row per covered recipient, attributes taken from the latest payment.

    CMS restates recipient attributes over time (specialty, address). Taking the
    most recent non-null value gives a current-state (Type 1) dimension.

    TODO(gold): promote ``recipient_specialty`` to Type 2 if CAS needs to test
    payments against the specialty as recorded at payment date.
    """
    ranked = silver.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("recipient_key")
            .orderBy(F.col("payment_date").desc_nulls_last(), F.col("payment_id").desc())
        ),
    )
    return (
        ranked.filter(F.col("_rn") == 1)
        .select(
            _surrogate_key("recipient_key").alias("recipient_sk"),
            "recipient_key",
            "recipient_type",
            "recipient_npi",
            F.col("recipient_first_name").alias("first_name"),
            F.col("recipient_last_name").alias("last_name"),
            F.concat_ws(" ", F.col("recipient_first_name"), F.col("recipient_last_name")).alias("full_name"),
            F.col("recipient_specialty").alias("specialty"),
            F.col("recipient_city").alias("city"),
            F.col("recipient_state").alias("state"),
            "teaching_hospital_id",
            "teaching_hospital_name",
            F.when(F.col("teaching_hospital_id").isNotNull(), F.lit("Teaching Hospital"))
             .otherwise(F.lit("Individual")).alias("recipient_class"),
        )
    )


def build_dim_manufacturer(silver: DataFrame) -> DataFrame:
    """One row per reporting entity, keyed on the normalised name.

    Keyed on ``manufacturer_name_norm`` rather than ``manufacturer_id`` because CMS
    issues multiple ids to the same corporate entity across program years, which
    would fragment peer groups.
    """
    return (
        silver.groupBy("manufacturer_name_norm")
        .agg(
            F.max("manufacturer_name").alias("manufacturer_name"),
            F.max("manufacturer_id").alias("manufacturer_id"),
            F.max("manufacturer_state").alias("manufacturer_state"),
        )
        .select(
            _surrogate_key("manufacturer_name_norm").alias("manufacturer_sk"),
            F.col("manufacturer_name_norm").alias("manufacturer_key"),
            "manufacturer_name",
            "manufacturer_id",
            "manufacturer_state",
        )
    )


def build_dim_nature(silver: DataFrame) -> DataFrame:
    """Nature-of-payment categories, flagged for audit relevance.

    ``is_high_scrutiny`` marks the categories where anti-kickback exposure
    concentrates -- speaking and consulting arrangements -- so the semantic model
    can express "high-scrutiny spend" as a single measure.
    """
    high_scrutiny = [
        "Compensation for services other than consulting, including serving as faculty or as a speaker at a venue other than a continuing education program",
        "Consulting Fee",
        "Compensation for serving as faculty or as a speaker for a non-accredited and noncertified continuing education program",
        "Honoraria",
    ]
    return (
        silver.select("nature_of_payment").distinct()
        .filter(F.col("nature_of_payment").isNotNull())
        .select(
            _surrogate_key("nature_of_payment").alias("nature_sk"),
            F.col("nature_of_payment").alias("nature_of_payment"),
            F.when(F.col("nature_of_payment").isin(high_scrutiny), F.lit(True))
             .otherwise(F.lit(False)).alias("is_high_scrutiny"),
        )
    )


def build_dim_date(silver: DataFrame) -> DataFrame:
    """Contiguous calendar dimension spanning the payment date range.

    Built from an explicit sequence rather than the distinct payment dates: a
    gapless date table is required for DAX time-intelligence to behave.
    """
    bounds = silver.agg(
        F.min("payment_date").alias("lo"),
        F.max("payment_date").alias("hi"),
    ).collect()[0]
    lo, hi = bounds["lo"], bounds["hi"]
    if lo is None or hi is None:
        return silver.sparkSession.createDataFrame([], T.StructType([
            T.StructField("date_sk", T.StringType()),
            T.StructField("date", T.DateType()),
        ]))
    return (
        silver.sparkSession.sql(
            f"SELECT explode(sequence(to_date('{lo}'), to_date('{hi}'), interval 1 day)) AS date"
        )
        .select(
            _surrogate_key("date").alias("date_sk"),
            "date",
            F.year("date").alias("calendar_year"),
            F.quarter("date").alias("calendar_quarter"),
            F.month("date").alias("calendar_month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.dayofmonth("date").alias("day_of_month"),
            F.dayofweek("date").alias("day_of_week"),
            F.date_format("date", "EEEE").alias("day_name"),
            # Round-number and period-end behaviour feed risk rule R4.
            F.when(F.col("date") == F.last_day("date"), F.lit(True)).otherwise(F.lit(False)).alias("is_month_end"),
        )
    )


def build_fact_payment(silver: DataFrame) -> DataFrame:
    """Narrow fact table carrying only keys, degenerate dimensions and measures."""
    return silver.select(
        F.col("payment_id"),
        _surrogate_key("recipient_key").alias("recipient_sk"),
        _surrogate_key("manufacturer_name_norm").alias("manufacturer_sk"),
        _surrogate_key("nature_of_payment").alias("nature_sk"),
        _surrogate_key("payment_date").alias("date_sk"),
        F.col("payment_date"),
        F.col("program_year"),
        F.col("payment_category"),
        F.col("amount_usd"),
        F.col("number_of_payments"),
        F.col("form_of_payment"),
        F.col("related_product_indicator"),
        F.col("dispute_indicator"),
        F.col("delay_in_publication"),
        # Retained for traceability from a Power BI row back to the source file.
        F.col("_ingest_run_id"),
    )


BUILDERS = {
    "dim_recipient": build_dim_recipient,
    "dim_manufacturer": build_dim_manufacturer,
    "dim_nature": build_dim_nature,
    "dim_date": build_dim_date,
    "fact_payment": build_fact_payment,
}
