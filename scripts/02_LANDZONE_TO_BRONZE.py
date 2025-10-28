import dlt
from pyspark.sql.functions import input_file_name

# Retrieve the storage account key from Databricks secrets.
storage_key = dbutils.secrets.get(scope="my-scope", key="adls-key")

# Configure Spark to use the Azure Data Lake Storage account key so it can access data.
spark.conf.set(
    "fs.azure.account.key.datalakequaliteeau.dfs.core.windows.net",
    storage_key
)

def create_bronze_table(folder_name, table_name):
    """
    Utility function to read all CSV files from a given ADLS folder into a Spark DataFrame.
    Returns DataFrame to be used as a Delta Live Table (DLT) bronze table.
    
    Args:
        folder_name (str): Subfolder name in the blob container (e.g., "PLV", "COM", "RESULT")
        table_name (str): The resulting table name (not actually used, but could help for logging)
    Returns:
        DataFrame: The loaded Spark DataFrame
    """
    path = f"abfss://source@datalakequaliteeau.dfs.core.windows.net/{folder_name}"
    return (
        spark.read.format("csv")
            .option("header", "true")
            .option("delimiter", ",")  
            .load(path)
    )

# Create the DLT bronze table for PLV data.
@dlt.table(
    name="bronze_plv",
    comment="PLV raw data, loaded as Bronze layer",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_plv():
    # Call utility function to read all PLV files.
    return create_bronze_table("PLV", "bronze_plv")

# Create the DLT bronze table for COM data.
@dlt.table(
    name="bronze_com",
    comment="COM raw data, loaded as Bronze layer",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_com():
    # Call utility function to read all COM files.
    return create_bronze_table("COM", "bronze_com")

# Create the DLT bronze table for RESULT data.
@dlt.table(
    name="bronze_result",
    comment="RESULT raw data, loaded as Bronze layer",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_result():
    # Call utility function to read all RESULT files.
    return create_bronze_table("RESULT", "bronze_result")