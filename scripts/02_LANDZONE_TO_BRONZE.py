import dlt
from pyspark.sql.functions import input_file_name

# Récupérer la clé dans un secret
storage_key = dbutils.secrets.get(scope="my-scope", key="adls-key")

# Configurer la clé dans Spark
spark.conf.set(
    "fs.azure.account.key.datalakequaliteeau.dfs.core.windows.net",
    storage_key
)

# Fonction pour charger tous les fichiers d'un dossier en une table bronze
def create_bronze_table(folder_name, table_name):
    path = f"abfss://source@datalakequaliteeau.dfs.core.windows.net/{folder_name}"
    return (
        spark.read.format("csv")
            .option("header", "true")   # si la première ligne est un header
            .option("delimiter", ",")  # ou ";" ou "," selon votre fichier
            .load(path)
            )

# Table PLV
@dlt.table(
    name="bronze_plv",
    comment="Données PLV",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_plv():
    return create_bronze_table("PLV", "bronze_plv")

# Table COM
@dlt.table(
    name="bronze_com",
    comment="Données COM",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_com():
    return create_bronze_table("COM", "bronze_com")

# Table RESULT
@dlt.table(
    name="bronze_result",
    comment="Données RESULT",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_result():
    return create_bronze_table("RESULT", "bronze_result")