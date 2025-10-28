import pytest
from pyspark.sql import SparkSession
from scripts.utils import clean_plv, clean_commune, clean_result  

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("pytest-pyspark").getOrCreate()

def test_clean_plv(spark_session):
    data = [
        {"cddept": "1", "cdreseau": "23", "heureprel": "08h34", "pourcentdebit": "25 %", "nomcommuneprinc": " Lyon "},
    ]
    df = spark_session.createDataFrame(data)
    result = clean_plv(df)
    row = result.collect()[0]
    assert row["cddept"] == "001"
    assert row["cdreseau"] == "000000023"
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
    input_data = [{"cddept":"1", "referenceprel":" ref1 ","limitequal":"3,4", "valtraduite":"2,5", "qualitparam":" c "}]
    df = spark_session.createDataFrame(input_data)
    result = clean_result(df).collect()[0]
    assert result["cddept"] == "001"
    assert result["referenceprel"] == "ref1"
    assert result["limitequal"] == 3.4
    assert result["valtraduite"] == 2.5
    assert result["qualitparam"] == "C"