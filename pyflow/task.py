from __future__ import annotations
import functools
import logging
from typing import Callable, Optional

from pyflow.context import PipelineContext

logger = logging.getLogger(__name__)


class Task:
    """Wraps a callable as a named, executable pipeline step."""

    def __init__(self, fn: Callable, name: Optional[str] = None) -> None:
        self.fn = fn
        self.name = name or fn.__name__
        functools.update_wrapper(self, fn)

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logger.info("Running task: %s", self.name)
        result = self.fn(ctx)
        # Allow tasks to return ctx explicitly or rely on mutation
        return result if result is not None else ctx

    def __call__(self, ctx: PipelineContext) -> PipelineContext:
        return self.run(ctx)

    def __repr__(self) -> str:  # pragma: no cover
        return f"Task(name={self.name!r})"


def task(fn: Optional[Callable] = None, *, name: Optional[str] = None):
    """Decorator that turns a function into a :class:`Task`.

    Can be used with or without arguments::

        @task
        def my_step(ctx): ...

        @task(name="clean")
        def clean_data(ctx): ...
    """
    if fn is not None:
        # Called as @task without parentheses
        return Task(fn)

    # Called as @task(...) with keyword args
    def decorator(func: Callable) -> Task:
        return Task(func, name=name)

    return decorator
