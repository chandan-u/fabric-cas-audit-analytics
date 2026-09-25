"""Silver-layer transforms: typing, cleansing, conforming, deduplication.

Kept out of the notebook so each step is unit-testable against small DataFrames
without a Lakehouse. Notebooks stay thin orchestration; the rules live here.

Design note -- General Payments and Research Payments arrive as separate CMS
extracts with different column sets but the same audit question ("what value moved
to this recipient, from whom, for what"). Silver conforms them onto one grain and
tags ``payment_category``, so a risk rule is written once rather than twice.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Canonical Silver payment grain: one row per source payment record.
SILVER_PAYMENT_COLUMNS = [
    "payment_id",
    "payment_category",
    "program_year",
    "payment_date",
    "amount_usd",
    "number_of_payments",
    "nature_of_payment",
    "form_of_payment",
    "recipient_key",
    "recipient_type",
    "recipient_npi",
    "recipient_first_name",
    "recipient_last_name",
    "recipient_specialty",
    "recipient_city",
    "recipient_state",
    "teaching_hospital_id",
    "teaching_hospital_name",
    "manufacturer_id",
    "manufacturer_name",
    "manufacturer_state",
    "related_product_indicator",
    "dispute_indicator",
    "delay_in_publication",
    "change_type",
]


def cast_types(df: DataFrame) -> DataFrame:
    """Apply Silver typing. Bronze holds strings; money and dates become real types here.

    Amount uses DECIMAL(18,2) rather than DOUBLE -- float drift is unacceptable when
    the number ends up in an audit finding.
    """
    return (
        df.withColumn("amount_usd", F.col("amount_usd").cast("decimal(18,2)"))
        .withColumn("number_of_payments", F.col("number_of_payments").cast("int"))
        .withColumn("program_year", F.col("program_year").cast("int"))
        .withColumn("payment_date", F.to_date(F.col("payment_date"), "MM/dd/yyyy"))
    )


def normalise_strings(df: DataFrame, columns: list[str]) -> DataFrame:
    """Trim whitespace and convert empty strings to NULL.

    CMS submitters pad fields inconsistently; without this the same manufacturer
    appears as several distinct dimension members.
    """
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))
    return df


def build_recipient_key(df: DataFrame) -> DataFrame:
    """Derive a stable recipient key across the three recipient types.

    CMS identifies covered recipients by ``Covered_Recipient_Profile_ID`` for
    individuals and by ``Teaching_Hospital_ID`` for institutions; neither is
    populated for both. A single surrogate key lets one fact table carry both.
    """
    return df.withColumn(
        "recipient_key",
        F.coalesce(
            F.concat_ws(":", F.lit("PHYS"), F.col("recipient_profile_id")),
            F.concat_ws(":", F.lit("HOSP"), F.col("teaching_hospital_id")),
            F.concat_ws(":", F.lit("NPI"), F.col("recipient_npi")),
            F.lit("UNKNOWN"),
        ),
    )


def normalise_manufacturer(df: DataFrame) -> DataFrame:
    """Conform manufacturer names for peer-group comparison.

    ``manufacturer_name_norm`` is the join/grouping key; the original is retained
    because an audit workpaper must quote the name as reported.

    TODO(silver): extend to a curated crosswalk so corporate families
    (e.g. Lilly USA LLC vs Eli Lilly and Company) roll up to one parent entity.
    """
    cleaned = F.upper(F.trim(F.col("manufacturer_name")))
    for suffix in [r",?\s+LLC$", r",?\s+INC\.?$", r",?\s+L\.?P\.?$", r",?\s+CORP(ORATION)?$",
                   r",?\s+CO\.?$", r",?\s+LTD\.?$", r",?\s+U\.?S\.?A?\.?$"]:
        cleaned = F.regexp_replace(cleaned, suffix, "")
    return df.withColumn("manufacturer_name_norm", F.trim(cleaned))


def deduplicate_payments(df: DataFrame, key: str = "payment_id") -> DataFrame:
    """Collapse restatements to one current row per payment.

    CMS republishes corrected records; ``Change_Type`` marks NEW / CHANGED /
    UNCHANGED / DELETE. Taking the most recently ingested non-deleted row keeps the
    current view, and dropping DELETEs is what makes population counts defensible.
    """
    ranked = df.withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy(key).orderBy(
                F.col("_ingest_timestamp").desc(),
                F.col("program_year").desc(),
            )
        ),
    )
    return (
        ranked.filter((F.col("_rn") == 1) & (F.upper(F.coalesce(F.col("change_type"), F.lit(""))) != "DELETE"))
        .drop("_rn")
    )


def conform_general_payments(bronze: DataFrame) -> DataFrame:
    """Map the Bronze General Payments extract onto the Silver payment grain."""
    return bronze.select(
        F.col("Record_ID").alias("payment_id"),
        F.lit("GENERAL").alias("payment_category"),
        F.col("Program_Year").alias("program_year"),
        F.col("Date_of_Payment").alias("payment_date"),
        F.col("Total_Amount_of_Payment_USDollars").alias("amount_usd"),
        F.col("Number_of_Payments_Included_in_Total_Amount").alias("number_of_payments"),
        F.col("Nature_of_Payment_or_Transfer_of_Value").alias("nature_of_payment"),
        F.col("Form_of_Payment_or_Transfer_of_Value").alias("form_of_payment"),
        F.col("Covered_Recipient_Profile_ID").alias("recipient_profile_id"),
        F.col("Covered_Recipient_Type").alias("recipient_type"),
        F.col("Covered_Recipient_NPI").alias("recipient_npi"),
        F.col("Covered_Recipient_First_Name").alias("recipient_first_name"),
        F.col("Covered_Recipient_Last_Name").alias("recipient_last_name"),
        F.col("Covered_Recipient_Specialty_1").alias("recipient_specialty"),
        F.col("Recipient_City").alias("recipient_city"),
        F.col("Recipient_State").alias("recipient_state"),
        F.col("Teaching_Hospital_ID").alias("teaching_hospital_id"),
        F.col("Teaching_Hospital_Name").alias("teaching_hospital_name"),
        F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID").alias("manufacturer_id"),
        F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name").alias("manufacturer_name"),
        F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State").alias("manufacturer_state"),
        F.col("Related_Product_Indicator").alias("related_product_indicator"),
        F.col("Dispute_Status_for_Publication").alias("dispute_indicator"),
        F.col("Delay_in_Publication_Indicator").alias("delay_in_publication"),
        F.col("Change_Type").alias("change_type"),
        F.col("_ingest_timestamp"),
        F.col("_ingest_run_id"),
    )


def conform_research_payments(bronze: DataFrame) -> DataFrame:
    """Map the Bronze Research Payments extract onto the Silver payment grain.

    TODO(silver): research records carry principal-investigator and study columns
    with no General Payments equivalent. Confirm column names against the Research
    extract header, then land the study attributes in a satellite Silver table
    keyed on ``payment_id`` rather than widening this grain.
    """
    return bronze.select(
        F.col("Record_ID").alias("payment_id"),
        F.lit("RESEARCH").alias("payment_category"),
        F.col("Program_Year").alias("program_year"),
        F.col("Date_of_Payment").alias("payment_date"),
        F.col("Total_Amount_of_Payment_USDollars").alias("amount_usd"),
        F.lit(1).alias("number_of_payments"),
        F.lit("Research").alias("nature_of_payment"),
        F.col("Form_of_Payment_or_Transfer_of_Value").alias("form_of_payment"),
        F.col("Covered_Recipient_Profile_ID").alias("recipient_profile_id"),
        F.col("Covered_Recipient_Type").alias("recipient_type"),
        F.col("Covered_Recipient_NPI").alias("recipient_npi"),
        F.col("Covered_Recipient_First_Name").alias("recipient_first_name"),
        F.col("Covered_Recipient_Last_Name").alias("recipient_last_name"),
        F.col("Covered_Recipient_Specialty_1").alias("recipient_specialty"),
        F.col("Recipient_City").alias("recipient_city"),
        F.col("Recipient_State").alias("recipient_state"),
        F.col("Teaching_Hospital_ID").alias("teaching_hospital_id"),
        F.col("Teaching_Hospital_Name").alias("teaching_hospital_name"),
        F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID").alias("manufacturer_id"),
        F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name").alias("manufacturer_name"),
        F.col("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State").alias("manufacturer_state"),
        F.col("Related_Product_Indicator").alias("related_product_indicator"),
        F.col("Dispute_Status_for_Publication").alias("dispute_indicator"),
        F.col("Delay_in_Publication_Indicator").alias("delay_in_publication"),
        F.col("Change_Type").alias("change_type"),
        F.col("_ingest_timestamp"),
        F.col("_ingest_run_id"),
    )


STRING_COLUMNS_TO_NORMALISE = [
    "nature_of_payment", "form_of_payment", "recipient_specialty",
    "recipient_city", "recipient_state", "manufacturer_name",
    "teaching_hospital_name", "recipient_first_name", "recipient_last_name",
]


def build_silver_payments(conformed: DataFrame) -> DataFrame:
    """Full Silver payment pipeline, composed from the steps above."""
    df = normalise_strings(conformed, STRING_COLUMNS_TO_NORMALISE)
    df = cast_types(df)
    df = build_recipient_key(df)
    df = normalise_manufacturer(df)
    df = deduplicate_payments(df)
    return df
