%pip install great_expectations

import great_expectations as gx
import pandas as pd
from IPython.display import display, HTML
import glob, os

context = gx.get_context(context_root_dir="/dbfs/great_expectations")

df = spark.table("GOLD.gold_sample_summary")
df = df.toPandas()

# Create Data Context.
context = gx.get_context()

# Connect to data.
# Create Data Source, Data Asset, Batch Definition, and Batch.
data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

suite = gx.ExpectationSuite(name="gold_sample_summary_suite")
suite = context.suites.add(suite)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=2016, max_value=2025, severity="warning")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="inseecommuneprinc")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(column="inseecommuneprinc", regex=r"^(?:\d{2}|2A|2B)\d{3}$")
)

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="nomcommuneprinc")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotMatchRegex(column="nomcommuneprinc", regex=r"^\s*$")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="year", min_value=2016, max_value=2025)
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="total_samples")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="compliance_rate", min_value=0.0, max_value=1.0)
)

validation_result = batch.validate(suite)
print(validation_result)
def print_ge_validation_report(validation_result):
    print("=== RAPPORT DE VALIDATION GREAT EXPECTATIONS ===")
    stats = validation_result["statistics"]
    print(f"Statut général : {'SUCCESS' if validation_result['success'] else 'FAILED'}")
    print(f"Taux de réussite : {stats['success_percent']:.1f}%")
    print(f"Nombre d'expectations évaluées : {stats['evaluated_expectations']}")
    print(f"Réussies  : {stats['successful_expectations']}")
    print(f"Ratées    : {stats['unsuccessful_expectations']}")
    print()

    for i, res in enumerate(validation_result["results"], 1):
        cfg = res["expectation_config"]
        exp_type = cfg["type"]
        col = cfg["kwargs"].get("column", "(Any)")
        min_v = cfg["kwargs"].get("min_value")
        max_v = cfg["kwargs"].get("max_value")
        severity = cfg.get("severity", "info")
        status = "SUCCESS" if res["success"] else "FAILED"
        result = res["result"]
        unexpected = result.get("unexpected_count", 0)
        unexpected_pct = result.get("unexpected_percent", 0.0)
        missing = result.get("missing_count", 0)
        n_total = result.get("element_count", "n/a")
        
        print(f"[{i}] {exp_type} sur colonne '{col}' (min={min_v}, max={max_v})")
        print(f"    Statut         : {status} (severity : {severity})")
        print(f"    Nb valeurs     : {n_total}")
        print(f"    Inattendues    : {unexpected} ({unexpected_pct:.2f}%)")
        print(f"    Valeurs manquantes : {missing}")
        # Afficher exemples inattendus s'il y en a
        partial_unx = result.get('partial_unexpected_list', [])
        if partial_unx:
            print(f"    Exemples de valeurs inattendues : {partial_unx}")
        print()
    print("=== FIN RAPPORT ===")

# Utilisation :
print_ge_validation_report(validation_result)