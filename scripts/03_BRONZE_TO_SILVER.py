import dlt
from pyspark.sql import functions as F
from pandas import DataFrame
from utils import clean_commune, clean_plv, clean_result, enrich_with_commune, enrich_with_prel

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