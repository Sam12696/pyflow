from __future__ import annotations
import logging
import time
from typing import List, Optional

from pyflow.context import PipelineContext
from pyflow.task import Task, task as task_decorator

logger = logging.getLogger(__name__)


class Pipeline:
    """Defines and executes an ordered ETL pipeline.

    Example::

        pipeline = Pipeline("sales_etl")

        @pipeline.task
        def clean(ctx):
            ctx.df = ctx.df.dropna()

        pipeline.source(CSVSource("data.csv"))
        pipeline.sink(JSONSink("output.json"))
        pipeline.run()
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._source = None
        self._sink = None
        self._tasks: List[Task] = []
        self._transforms: List = []

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    def source(self, src) -> "Pipeline":
        """Set the data source for this pipeline."""
        self._source = src
        return self

    def sink(self, snk) -> "Pipeline":
        """Set the data sink for this pipeline."""
        self._sink = snk
        return self

    def transform(self, *transforms) -> "Pipeline":
        """Add one or more transform steps."""
        self._transforms.extend(transforms)
        return self

    def task(self, fn=None, *, name: Optional[str] = None):
        """Register a function as a pipeline task (decorator)."""
        t = task_decorator(fn, name=name) if fn is not None else task_decorator(name=name)
        if isinstance(t, Task):
            self._tasks.append(t)
            return t
        # Called with arguments — return a decorator
        def decorator(func):
            inner = t(func)
            self._tasks.append(inner)
            return inner
        return decorator

    def add_task(self, t: Task) -> "Pipeline":
        """Programmatically append a :class:`Task`."""
        self._tasks.append(t)
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self) -> PipelineContext:
        """Execute the pipeline end-to-end and return the final context."""
        logger.info("=" * 50)
        logger.info("Starting pipeline: %s", self.name)
        start = time.perf_counter()

        ctx = PipelineContext(self.name)

        # 1. Extract
        if self._source is None:
            raise RuntimeError(f"Pipeline '{self.name}' has no source configured.")
        logger.info("Extracting from %s", type(self._source).__name__)
        ctx.df = self._source.read()

        # 2. Built-in transforms
        for transform in self._transforms:
            logger.info("Applying transform: %s", type(transform).__name__)
            ctx.df = transform.apply(ctx.df)

        # 3. Custom tasks
        for t in self._tasks:
            ctx = t.run(ctx)

        # 4. Load
        if self._sink is not None:
            logger.info("Writing to %s", type(self._sink).__name__)
            self._sink.write(ctx.df)

        elapsed = time.perf_counter() - start
        logger.info("Pipeline '%s' completed in %.3fs", self.name, elapsed)
        logger.info("=" * 50)
        return ctx

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Pipeline(name={self.name!r}, tasks={len(self._tasks)}, "
            f"transforms={len(self._transforms)})"
        )
