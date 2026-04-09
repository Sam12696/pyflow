"""Example: Read a CSV, clean it, and write to JSON.

Run from the repo root:
    python examples/csv_to_json.py
"""
import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

from pyflow import Pipeline
from pyflow.sources import CSVSource
from pyflow.sinks import JSONSink, StdoutSink
from pyflow.transforms import FilterTransform, RenameTransform, CastTransform

# ── Create sample data ────────────────────────────────────────────────
sample_csv = Path("examples/sample_customers.csv")
sample_csv.parent.mkdir(exist_ok=True)
sample_csv.write_text(
    "cust_id,name,age,salary,joined\n"
    "1,Alice,30,75000,2021-03-15\n"
    "2,Bob,,50000,2019-08-22\n"       # missing age — will be dropped
    "3,Charlie,25,60000,2022-11-01\n"
    "4,Diana,45,120000,2018-06-10\n"
    "5,Eve,17,15000,2023-01-05\n"     # under 18 — will be filtered
)

# ── Build the pipeline ────────────────────────────────────────────────
pipeline = Pipeline("customer_etl")

pipeline.source(CSVSource(sample_csv))

pipeline.transform(
    RenameTransform({"cust_id": "customer_id", "salary": "annual_salary"}),
    CastTransform({"annual_salary": float}),
    FilterTransform("age >= 18"),
)


@pipeline.task
def drop_nulls(ctx):
    before = len(ctx.df)
    ctx.df = ctx.df.dropna(subset=["age"]).reset_index(drop=True)
    print(f"  Dropped {before - len(ctx.df)} rows with missing age.")
    return ctx


@pipeline.task
def add_salary_band(ctx):
    ctx.df["salary_band"] = pd.cut(
        ctx.df["annual_salary"],
        bins=[0, 50_000, 100_000, float("inf")],
        labels=["low", "medium", "high"],
    )
    return ctx


pipeline.sink(JSONSink("examples/output_customers.json"))

ctx = pipeline.run()

StdoutSink(title="Final output").write(ctx.df)
print("\nDone! Output written to examples/output_customers.json")
