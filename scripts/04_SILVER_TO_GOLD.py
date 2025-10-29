import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------
# GOLD 1: Aggregated summary of sampling results per commune/year
# --------------------------------------------
@dlt.table(
    name="gold_sample_summary",
    comment="Summary per commune/year of sampling events & compliance rates"
)
def gold_sample_summary():
    df = dlt.read("SILVER.silver_plv")
    df = df.withColumn("year", F.year("dateprel"))
    # Compliance: both bacteriological and chemical marked "C"
    df = df.withColumn("is_compliant", (
        (F.col("plvconformitebacterio") == "C") & (F.col("plvconformitechimique") == "C")
    ))
    return (
        df.groupBy("inseecommuneprinc", "nomcommuneprinc", "year")
          .agg(
              F.count("*").alias("total_samples"),
              F.sum(F.col("is_compliant").cast("int")).alias("compliant_samples"),
              (F.count("*") - F.sum(F.col("is_compliant").cast("int"))).alias("non_compliant_samples"),
              (F.sum(F.col("is_compliant").cast("int")) / F.count("*")).alias("compliance_rate")
          )
    )

# --------------------------------------------
# GOLD 2: Per-parameter stats (min, max, avg, compliance), by commune and year
# --------------------------------------------
@dlt.table(
    name="gold_param_stats",
    comment="Statistics for each quality parameter by commune/year"
)
def gold_param_stats():
    df = dlt.read("SILVER.silver_result")
    df = df.withColumn("year", F.year("dateprel"))
    # Compliance: value must be <= limit and qualitative flag "C"
    df = df.withColumn(
        "is_compliant",
        (F.col("valtraduite") <= F.col("limitequal")) & (F.col("qualitparam") == "C")
    )
    return (
        df.groupBy("inseecommuneprinc", "nomcommuneprinc", "year", "cdparametre", "libmajparametre")
        .agg(
            F.min("valtraduite").alias("min_value"),
            F.max("valtraduite").alias("max_value"),
            F.avg("valtraduite").alias("avg_value"),
            F.sum(F.col("is_compliant").cast("int")).alias("compliant_count"),
            (F.count("*") - F.sum(F.col("is_compliant").cast("int"))).alias("non_compliant_count")
        )
    )

# --------------------------------------------
# GOLD 3: All events (sampling/results) where non-conformity is detected
# --------------------------------------------
@dlt.table(
    name="gold_non_compliance_events",
    comment="All non-compliance events with details"
)
def gold_non_compliance_events():
    df = dlt.read("SILVER.silver_result")
    # Non-compliance: qualitative "N" or value above limit
    return df.filter(
        (F.col("qualitparam") == "N") | (F.col("valtraduite") > F.col("limitequal"))
    ).select(
        "referenceprel",
        "inseecommuneprinc",
        "nomcommuneprinc",
        "dateprel",
        "cdparametre",
        "libmajparametre",
        "valtraduite",
        "limitequal",
        "qualitparam"
    )

# --------------------------------------------
# GOLD 4: Time series for selected key parameters per commune
# --------------------------------------------
@dlt.table(
    name="gold_trend_parametre",
    comment="Time series of key parameters by commune"
)
def gold_trend_parametre():
    df = dlt.read("SILVER.silver_result")
    key_params = ["NO3", "ECOLI", "BSIR"]  # adjust as needed
    return df.filter(
        F.col("cdparametre").isin(key_params)
    ).select(
        "inseecommuneprinc",
        "nomcommuneprinc",
        "cdparametre",
        "libmajparametre",
        "dateprel",
        "valtraduite"
    )

# --------------------------------------------
# GOLD 5: Latest compliance status per commune/network
# --------------------------------------------
@dlt.table(
    name="gold_latest_status",
    comment="Latest compliance status per commune/network"
)
def gold_latest_status():
    df = dlt.read("SILVER.silver_result")
    # Window by commune and network, ordered by descending date
    window = Window.partitionBy("inseecommuneprinc", "cdreseau").orderBy(F.desc("dateprel"))
    latest = (
        df.withColumn("rn", F.row_number().over(window))
          .filter(F.col("rn") == 1)
          .select(
              "inseecommuneprinc",
              "nomcommuneprinc",
              "cdreseau",
              "nomreseau",
              "dateprel",
              "qualitparam",
              "libmajparametre",
              "valtraduite",
              "limitequal"
          )
    )
    return latest

# --------------------------------------------
# GOLD 6: Alerts for communes/networks with recent or repeated non-compliance
# --------------------------------------------
@dlt.table(
    name="gold_alerts",
    comment="Alerts for repeated or recent non-compliance events"
)
def gold_alerts():
    df = dlt.read("GOLD.gold_non_compliance_events")
    df = df.withColumn("year", F.year("dateprel"))
    agg = df.groupBy("inseecommuneprinc", "nomcommuneprinc", "year").agg(
        F.count("*").alias("alerts_count")
    ).withColumn(
        "alert_flag", (F.col("alerts_count") > 2)  # Example: more than 2 non-compliances triggers an alert
    )
    return agg