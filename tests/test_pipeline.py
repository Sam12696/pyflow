"""Tests for Pipeline, Task, and PipelineContext."""
import pandas as pd
import pytest

from pyflow import Pipeline, Task, PipelineContext
from pyflow.sinks import StdoutSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _DummySource:
    def read(self):
        return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


class _DummySink:
    def __init__(self):
        self.written = None

    def write(self, df):
        self.written = df.copy()


# ---------------------------------------------------------------------------
# PipelineContext
# ---------------------------------------------------------------------------

class TestPipelineContext:
    def test_set_get(self):
        ctx = PipelineContext("test")
        ctx.set("key", "value")
        assert ctx.get("key") == "value"

    def test_get_default(self):
        ctx = PipelineContext("test")
        assert ctx.get("missing", 42) == 42

    def test_df_initially_none(self):
        ctx = PipelineContext("test")
        assert ctx.df is None


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------

class TestTask:
    def test_task_runs(self):
        ctx = PipelineContext("test")
        ctx.df = pd.DataFrame({"x": [1, 2]})

        def double(ctx):
            ctx.df["x"] = ctx.df["x"] * 2
            return ctx

        t = Task(double)
        result = t.run(ctx)
        assert list(result.df["x"]) == [2, 4]

    def test_task_mutation_without_return(self):
        ctx = PipelineContext("test")
        ctx.df = pd.DataFrame({"x": [10]})

        def mutate(ctx):
            ctx.df["x"] = 99
            # no return

        t = Task(mutate)
        result = t.run(ctx)
        assert result.df["x"].iloc[0] == 99


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class TestPipeline:
    def _basic_pipeline(self):
        sink = _DummySink()
        p = Pipeline("test_pipeline")
        p.source(_DummySource())
        p.sink(sink)
        return p, sink

    def test_run_produces_dataframe(self):
        p, sink = self._basic_pipeline()
        ctx = p.run()
        assert isinstance(ctx.df, pd.DataFrame)
        assert list(ctx.df.columns) == ["a", "b"]
        assert len(ctx.df) == 3

    def test_sink_receives_data(self):
        p, sink = self._basic_pipeline()
        p.run()
        assert sink.written is not None
        assert len(sink.written) == 3

    def test_task_decorator(self):
        p, _ = self._basic_pipeline()
        called = []

        @p.task
        def my_task(ctx):
            called.append(True)
            return ctx

        p.run()
        assert len(called) == 1

    def test_multiple_tasks_ordered(self):
        p, _ = self._basic_pipeline()
        order = []

        @p.task
        def first(ctx):
            order.append("first")
            return ctx

        @p.task
        def second(ctx):
            order.append("second")
            return ctx

        p.run()
        assert order == ["first", "second"]

    def test_no_source_raises(self):
        p = Pipeline("no_source")
        with pytest.raises(RuntimeError, match="no source"):
            p.run()

    def test_pipeline_without_sink(self):
        """Pipeline should complete fine with no sink."""
        p = Pipeline("no_sink")
        p.source(_DummySource())
        ctx = p.run()
        assert ctx.df is not None
