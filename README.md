# pyflow

> A lightweight Python framework for building and running ETL data pipelines.

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Why pyflow?

Airflow and Prefect are powerful — but they're heavy. pyflow is a framework for building clean, testable ETL pipelines in pure Python, with no daemons, no databases, and no YAML.

Define your pipeline in code. Run it anywhere.

---

## Requirements

- Python 3.9 or higher
- pip

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/Sam12696/pyflow.git
cd pyflow
```

### 2. (Recommended) Create a virtual environment

```bash
# Create the environment
python -m venv venv

# Activate it
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### 3. Install the package and its dependencies

```bash
pip install -e .
```

This installs pyflow in editable mode so you can import it from anywhere inside the project.

> **Optional — Postgres support:**
> ```bash
> pip install sqlalchemy psycopg2-binary
> ```

---

## Running the Examples

Two ready-to-run demo pipelines are included in the `examples/` folder.

### Example 1 — CSV to JSON (with cleaning and enrichment)

Reads a CSV of customer records, renames columns, casts types, filters out rows with missing or invalid data, adds a computed `salary_band` column, and writes the result to JSON.

```bash
python examples/csv_to_json.py
```

Expected output:

```
INFO  Starting pipeline: customer_etl
INFO  Extracting from CSVSource
INFO  Applying transform: RenameTransform
INFO  Applying transform: CastTransform
INFO  Applying transform: FilterTransform
INFO  Running task: drop_nulls
INFO  Running task: add_salary_band
INFO  Writing to JSONSink
INFO  Pipeline 'customer_etl' completed in 0.19s

 customer_id    name  age  annual_salary     joined salary_band
           1   Alice 30.0        75000.0 2021-03-15      medium
           3 Charlie 25.0        60000.0 2022-11-01      medium
           4   Diana 45.0       120000.0 2018-06-10        high

Done! Output written to examples/output_customers.json
```

### Example 2 — REST API to CSV

Fetches posts from a public REST API, filters by user, renames columns, truncates long text, and writes to CSV. Requires an internet connection.

```bash
python examples/api_to_csv.py
```

Expected output:

```
INFO  Starting pipeline: posts_etl
INFO  Extracting from RESTSource
INFO  Applying transform: FilterTransform
INFO  Applying transform: RenameTransform
INFO  Running task: truncate_body
INFO  Writing to CSVSink
INFO  Pipeline 'posts_etl' completed in 0.38s

Done! Output written to examples/output_posts.csv
```

---

## Running the Tests

```bash
# Install test dependencies (if not already installed)
pip install pytest pytest-cov

# Run all tests
python -m pytest tests/ -v
```

Expected output:

```
collected 29 items

tests/test_pipeline.py::TestPipelineContext::test_set_get PASSED
tests/test_pipeline.py::TestPipelineContext::test_get_default PASSED
tests/test_pipeline.py::TestPipelineContext::test_df_initially_none PASSED
tests/test_pipeline.py::TestTask::test_task_runs PASSED
tests/test_pipeline.py::TestTask::test_task_mutation_without_return PASSED
tests/test_pipeline.py::TestPipeline::test_run_produces_dataframe PASSED
tests/test_pipeline.py::TestPipeline::test_sink_receives_data PASSED
tests/test_pipeline.py::TestPipeline::test_task_decorator PASSED
tests/test_pipeline.py::TestPipeline::test_multiple_tasks_ordered PASSED
tests/test_pipeline.py::TestPipeline::test_no_source_raises PASSED
tests/test_pipeline.py::TestPipeline::test_pipeline_without_sink PASSED
tests/test_sources.py::TestCSVSource::test_reads_csv PASSED
tests/test_sources.py::TestCSVSource::test_missing_file_raises PASSED
tests/test_sources.py::TestCSVSource::test_custom_delimiter PASSED
tests/test_sources.py::TestJSONSource::test_reads_json PASSED
tests/test_sources.py::TestJSONSource::test_missing_file_raises PASSED
tests/test_transforms.py::TestFilterTransform::test_callable_filter PASSED
tests/test_transforms.py::TestFilterTransform::test_query_string_filter PASSED
tests/test_transforms.py::TestFilterTransform::test_index_reset PASSED
tests/test_transforms.py::TestMapTransform::test_column_map PASSED
tests/test_transforms.py::TestMapTransform::test_dataframe_map PASSED
tests/test_transforms.py::TestMapTransform::test_original_not_mutated PASSED
tests/test_transforms.py::TestRenameTransform::test_rename PASSED
tests/test_transforms.py::TestRenameTransform::test_partial_rename PASSED
tests/test_transforms.py::TestCastTransform::test_cast_to_int PASSED
tests/test_transforms.py::TestCastTransform::test_cast_to_float PASSED
tests/test_transforms.py::TestCastTransform::test_cast_datetime PASSED
tests/test_transforms.py::TestCastTransform::test_missing_column_raises PASSED
tests/test_transforms.py::TestCastTransform::test_original_not_mutated PASSED

29 passed in 2.21s
```

### Run with coverage report

```bash
python -m pytest tests/ -v --cov=pyflow --cov-report=term-missing
```

---

## Usage

### Minimal pipeline

```python
from pyflow import Pipeline
from pyflow.sources import CSVSource
from pyflow.sinks import JSONSink

pipeline = Pipeline("my_pipeline")
pipeline.source(CSVSource("data/input.csv"))
pipeline.sink(JSONSink("data/output.json"))
pipeline.run()
```

### Full pipeline with transforms and custom tasks

```python
from pyflow import Pipeline
from pyflow.sources import CSVSource
from pyflow.sinks import JSONSink
from pyflow.transforms import FilterTransform, RenameTransform, CastTransform

pipeline = Pipeline("sales_etl")

# 1. Source
pipeline.source(CSVSource("data/sales.csv"))

# 2. Declarative transforms — applied in order
pipeline.transform(
    RenameTransform({"cust_id": "customer_id", "amt": "amount"}),
    CastTransform({"amount": float}),
    FilterTransform("amount > 0"),
)

# 3. Custom task — full Python, do anything
@pipeline.task
def add_tax(ctx):
    ctx.df["amount_with_tax"] = ctx.df["amount"] * 1.2
    return ctx

# 4. Sink
pipeline.sink(JSONSink("output/sales.json"))

# Run
ctx = pipeline.run()
print(f"Processed {len(ctx.df)} rows.")
```

### Fetch from a REST API

```python
from pyflow import Pipeline
from pyflow.sources import RESTSource
from pyflow.sinks import CSVSink

pipeline = Pipeline("api_etl")
pipeline.source(RESTSource("https://api.example.com/data", data_key="results"))
pipeline.sink(CSVSink("output/data.csv"))
pipeline.run()
```

### Read from / write to PostgreSQL

```python
from pyflow.sources import PostgresSource
from pyflow.sinks import PostgresSink

DSN = "postgresql://user:password@localhost:5432/mydb"

pipeline.source(PostgresSource(DSN, "SELECT * FROM orders WHERE status = 'pending'"))
pipeline.sink(PostgresSink(DSN, table="orders_processed", if_exists="append"))
```

---

## How It Works

Every pipeline follows the same four-step execution model:

```
Source  →  Transforms  →  Tasks  →  Sink
```

| Step | What happens |
|---|---|
| **Source** | Reads raw data and returns a pandas DataFrame |
| **Transforms** | Declarative, chainable operations (rename, cast, filter, map) |
| **Tasks** | Your custom Python functions — full control, runs in order |
| **Sink** | Writes the final DataFrame to its destination |

All steps share a `PipelineContext` object (`ctx`) that carries:
- `ctx.df` — the working DataFrame
- `ctx.set(key, value)` / `ctx.get(key)` — metadata you want to pass between tasks

---

## Sources

| Source | Description |
|---|---|
| `CSVSource(path)` | Read a CSV file |
| `JSONSource(path)` | Read a JSON file |
| `RESTSource(url, data_key=, next_key=)` | Fetch from a REST API with optional auto-pagination |
| `PostgresSource(dsn, query)` | Run a SQL query against PostgreSQL |

## Transforms

| Transform | Description |
|---|---|
| `FilterTransform(condition)` | Keep rows matching a lambda or pandas query string |
| `MapTransform(fn, column=)` | Apply a function to a column or the entire DataFrame |
| `RenameTransform(mapping)` | Rename columns via a `{old: new}` dict |
| `CastTransform(schema)` | Cast columns to target dtypes (`int`, `float`, `datetime`, etc.) |

## Sinks

| Sink | Description |
|---|---|
| `CSVSink(path)` | Write to CSV |
| `JSONSink(path)` | Write to JSON (pretty-printed) |
| `PostgresSink(dsn, table)` | Insert into a PostgreSQL table |
| `StdoutSink()` | Print to terminal — useful for debugging |

---

## Project Structure

```
pyflow/
├── pyflow/
│   ├── __init__.py       # Public API
│   ├── pipeline.py       # Pipeline class — orchestrates all steps
│   ├── task.py           # Task class + @task decorator
│   ├── context.py        # PipelineContext — shared state between steps
│   ├── sources/          # CSVSource, JSONSource, RESTSource, PostgresSource
│   ├── sinks/            # CSVSink, JSONSink, PostgresSink, StdoutSink
│   └── transforms/       # FilterTransform, MapTransform, RenameTransform, CastTransform
├── examples/
│   ├── csv_to_json.py    # Demo: CSV → clean → JSON
│   └── api_to_csv.py     # Demo: REST API → CSV
├── tests/
│   ├── test_pipeline.py  # Pipeline, Task, PipelineContext tests
│   ├── test_sources.py   # CSV and JSON source tests
│   └── test_transforms.py # All transform tests
├── setup.py
└── requirements.txt
```

---

## License

MIT
