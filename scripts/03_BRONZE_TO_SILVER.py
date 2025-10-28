import dlt
from pyspark.sql import functions as F

# ----------- Functions --------------- 

def clean_commune(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("inseecommune", F.lpad(F.trim(F.col("inseecommune")), 5, "0"))
        .withColumn("nomcommune", F.upper(F.trim(F.col("nomcommune"))))
        .withColumn("quartier", F.when(F.trim(F.col("quartier")).isin(["-", "", None, "null"]), None)
                                  .otherwise(F.trim(F.col("quartier"))))
        .withColumn("cdreseau", F.lpad(F.trim(F.col("cdreseau")), 9, "0"))
        .withColumn("nomreseau", F.upper(F.trim(F.col("nomreseau"))))
        .withColumn("debutalim", F.to_date(F.col("debutalim"), "yyyy-MM-dd"))
        .dropDuplicates()
    )
def clean_plv(df: DataFrame) -> DataFrame:
    """Clean and standardize sampling event columns from PLV bronze DataFrame."""
    return (
        df
        .withColumn("cddept", F.lpad(F.trim(F.col("cddept")), 3, "0"))
        .withColumn("cdreseau", F.lpad(F.trim(F.col("cdreseau")), 9, "0"))
        .withColumn("inseecommuneprinc", F.lpad(F.trim(F.col("inseecommuneprinc")), 5, "0"))
        .withColumn("nomcommuneprinc", F.upper(F.trim(F.col("nomcommuneprinc"))))
        .withColumn("cdreseauamont", F.lpad(F.trim(F.col("cdreseauamont")), 9, "0"))
        .withColumn("nomreseauamont", F.upper(F.trim(F.col("nomreseauamont"))))
        .withColumn("pourcentdebit", F.regexp_replace(F.col("pourcentdebit"), "[^0-9]", "").cast("int"))
        .withColumn("referenceprel", F.trim(F.col("referenceprel")))
        .withColumn("dateprel", F.to_date(F.col("dateprel"), "yyyy-MM-dd"))
        .withColumn("heureprel", F.regexp_replace(F.col("heureprel"), r"^(\d{2})h(\d{2})$", r"\1:\2"))
        .withColumn("conclusionprel", F.trim(F.col("conclusionprel")))
        .withColumn("ugelib", F.trim(F.col("ugelib")))
        .withColumn("distrlib", F.trim(F.col("distrlib")))
        .withColumn("moalib", F.trim(F.col("moalib")))
        .replace(["-", "", "null"], None)
        .dropDuplicates()
    )

def enrich_with_commune(plv: DataFrame, commune: DataFrame) -> DataFrame:
    """Join PLV sampling events with commune info using network code, removing duplicate cdreseau."""
    joined = plv.join(
        commune,
        [plv.cdreseau == commune.cdreseau],
        how="left"
    )
    # Remove the duplicate cdreseau column from commune
    return joined.drop(commune.cdreseau)

def clean_result(df: DataFrame) -> DataFrame:
    """Clean and standardize analytical result columns from bronze_result DataFrame."""
    return (
        df
        .withColumn("cddept", F.lpad(F.trim(F.col("cddept")), 3, "0"))
        .withColumn("referenceprel", F.trim(F.col("referenceprel")))
        .withColumn("cdparametresiseeaux", F.trim(F.col("cdparametresiseeaux")))
        .withColumn("cdparametre", F.trim(F.col("cdparametre")))
        .withColumn("libmajparametre", F.trim(F.col("libmajparametre")))
        .withColumn("libminparametre", F.trim(F.col("libminparametre")))
        .withColumn("libwebparametre", F.trim(F.col("libwebparametre")))
        .withColumn("qualitparam", F.upper(F.trim(F.col("qualitparam"))))
        .withColumn("insituana", F.trim(F.col("insituana")))
        .withColumn("rqana", F.trim(F.col("rqana")))
        .withColumn("cdunitereferencesiseeaux", F.trim(F.col("cdunitereferencesiseeaux")))
        .withColumn("cdunitereference", F.trim(F.col("cdunitereference")))
        .withColumn("limitequal", F.regexp_replace(F.col("limitequal"), ",", ".").cast("double"))
        .withColumn("refqual", F.regexp_replace(F.col("refqual"), ",", ".").cast("double"))
        .withColumn("valtraduite", F.regexp_replace(F.col("valtraduite"), ",", ".").cast("double"))
        .withColumn("casparam", F.trim(F.col("casparam")))
        .withColumn("referenceanl", F.trim(F.col("referenceanl")))
        .replace(["-", "", "null"], None)
        .dropDuplicates()
    )

def enrich_with_prel(result: DataFrame, prel: DataFrame) -> DataFrame:
    """Join analytical results with sampling events on department and sampling reference, drop duplicate keys."""
    joined = result.join(
        prel,
        [result.cddept == prel.cddept, result.referenceprel == prel.referenceprel],
        how="left"
    )
    # Remove duplicate join keys from prel
    return joined.drop(prel.cddept, prel.referenceprel)

# ----------- Silver commune/network info (from bronze_com) --------------
@dlt.table(
    name="silver_com",
    comment="Cleaned commune and network info (silver)",
    table_properties={"quality": "silver"}
)
def silver_com():
    df = dlt.read("BRONZE.bronze_com")
    df = clean_commune(df)
    return df

# --------- Silver sampling events (bronze_plv) enriched with commune/network ----------
@dlt.table(
    name="silver_plv",
    comment="Cleaned sampling events, enriched with commune info (silver)",
    table_properties={"quality": "silver"}
)
def silver_plv():
    plv = dlt.read("BRONZE.bronze_plv")
    commune = dlt.read("silver_com")
    plv_clean = clean_plv(plv)
    plv_enriched = enrich_with_commune(plv_clean, commune)
    return plv_enriched

# ---------- Silver analytical results (bronze_result) joined with sampling ----------
@dlt.table(
    name="silver_result",
    comment="Cleaned analytical results, joined with sampled events (silver)",
    table_properties={"quality": "silver"}
)
def silver_result():
    result = dlt.read("BRONZE.bronze_result")
    prel = dlt.read("silver_plv")
    result_clean = clean_result(result)
    result_enriched = enrich_with_prel(result_clean, prel)
    return result_enriched