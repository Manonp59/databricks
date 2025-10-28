import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from scripts.utils import clean_plv, clean_commune, clean_result  

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("pytest-pyspark").getOrCreate()

def test_clean_plv(spark_session):
    data = [
        {
            "cddept": "1",
            "cdreseau": "23",
            "inseecommuneprinc": "7",              
            "nomcommuneprinc": " Lyon ",
            "cdreseauamont": "",
            "nomreseauamont": None,
            "pourcentdebit": "25 %",
            "referenceprel": None,
            "dateprel": None,
            "heureprel": "08h34",
            "conclusionprel": None,
            "ugelib": None,
            "distrlib": None,
            "moalib": None
        },
    ]
    schema = StructType([
    StructField("cddept", StringType()),
    StructField("cdreseau", StringType()),
    StructField("inseecommuneprinc", StringType()),
    StructField("nomcommuneprinc", StringType()),
    StructField("cdreseauamont", StringType()),
    StructField("nomreseauamont", StringType()),
    StructField("pourcentdebit", StringType()),
    StructField("referenceprel", StringType()),
    StructField("dateprel", StringType()),
    StructField("heureprel", StringType()),
    StructField("conclusionprel", StringType()),
    StructField("ugelib", StringType()),
    StructField("distrlib", StringType()),
    StructField("moalib", StringType()),
    ])
    df = spark_session.createDataFrame(data, schema=schema)
    result = clean_plv(df)
    row = result.collect()[0]
    assert row["cddept"] == "001"
    assert row["cdreseau"] == "000000023"
    assert row["inseecommuneprinc"] == "00007"
    assert row["heureprel"] == "08:34"
    assert row["pourcentdebit"] == 25
    assert row["nomcommuneprinc"] == "LYON"

def test_clean_commune(spark_session):
    data = [
        {"inseecommune": "123", "nomcommune": "  pry ", "quartier": "-", "cdreseau": "456", "nomreseau": "rA seau", "debutalim": "2023-01-05"},
        {"inseecommune": "00085", "nomcommune": None, "quartier": None, "cdreseau": None, "nomreseau": None, "debutalim": None},
    ]
    df = spark_session.createDataFrame(data)
    result = clean_commune(df)
    rows = result.collect()
    assert rows[0]["inseecommune"] == "00123"
    assert rows[0]["quartier"] is None
    assert rows[0]["nomcommune"] == "PRY"
    assert str(rows[0]["debutalim"]) == "2023-01-05"

def test_clean_result(spark_session):
    schema = StructType([
    StructField("cddept", StringType()),
    StructField("referenceprel", StringType()),
    StructField("cdparametresiseeaux", StringType()),
    StructField("cdparametre", StringType()),
    StructField("libmajparametre", StringType()),
    StructField("libminparametre", StringType()),
    StructField("libwebparametre", StringType()),
    StructField("qualitparam", StringType()),
    StructField("insituana", StringType()),
    StructField("rqana", StringType()),
    StructField("cdunitereferencesiseeaux", StringType()),
    StructField("cdunitereference", StringType()),
    StructField("limitequal", StringType()),
    StructField("refqual", StringType()),
    StructField("valtraduite", StringType()),
    StructField("casparam", StringType()),
    StructField("referenceanl", StringType()),
    ])
    input_data = [{
        "cddept":"1",
        "referenceprel":" ref1 ",
        "cdparametresiseeaux": None,
        "cdparametre": None,
        "libmajparametre": None,
        "libminparametre": None,
        "libwebparametre": None,
        "qualitparam":" c ",
        "insituana": None,
        "rqana": None,
        "cdunitereferencesiseeaux": None,
        "cdunitereference": None,
        "limitequal":"3,4",
        "refqual": None,
        "valtraduite":"2,5",
        "casparam": None,
        "referenceanl": None
    }]
    df = spark_session.createDataFrame(input_data, schema=schema)
    result = clean_result(df).collect()[0]
    assert result["cddept"] == "001"
    assert result["referenceprel"] == "ref1"
    assert result["limitequal"] == 3.4
    assert result["valtraduite"] == 2.5
    assert result["qualitparam"] == "C"