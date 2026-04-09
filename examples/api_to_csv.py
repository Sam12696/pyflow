"""Example: Fetch data from a public REST API and write to CSV.

Uses https://jsonplaceholder.typicode.com — no auth required.

Run from the repo root:
    python examples/api_to_csv.py
"""
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

from pyflow import Pipeline
from pyflow.sources import RESTSource
from pyflow.sinks import CSVSink, StdoutSink
from pyflow.transforms import FilterTransform, RenameTransform

pipeline = Pipeline("posts_etl")

pipeline.source(
    RESTSource(
        url="https://jsonplaceholder.typicode.com/posts",
        data_key=None,      # root is already a list
    )
)

pipeline.transform(
    FilterTransform(lambda df: df["userId"] <= 3),   # first 3 users only
    RenameTransform({"userId": "user_id", "id": "post_id"}),
)


@pipeline.task
def truncate_body(ctx):
    ctx.df["body"] = ctx.df["body"].str[:80] + "…"
    return ctx


pipeline.sink(CSVSink("examples/output_posts.csv"))

ctx = pipeline.run()
StdoutSink(max_rows=10, title="Posts (first 10)").write(ctx.df)
print("\nDone! Output written to examples/output_posts.csv")
