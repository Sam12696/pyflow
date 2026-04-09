"""Tests for file-based sources."""
import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from pyflow.sources import CSVSource, JSONSource


class TestCSVSource:
    def test_reads_csv(self, tmp_path):
        p = tmp_path / "data.csv"
        p.write_text("a,b\n1,2\n3,4\n")
        df = CSVSource(p).read()
        assert list(df.columns) == ["a", "b"]
        assert len(df) == 2

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            CSVSource(tmp_path / "missing.csv").read()

    def test_custom_delimiter(self, tmp_path):
        p = tmp_path / "data.tsv"
        p.write_text("a\tb\n1\t2\n")
        df = CSVSource(p, delimiter="\t").read()
        assert list(df.columns) == ["a", "b"]


class TestJSONSource:
    def test_reads_json(self, tmp_path):
        p = tmp_path / "data.json"
        p.write_text(json.dumps([{"x": 1}, {"x": 2}]))
        df = JSONSource(p).read()
        assert "x" in df.columns
        assert len(df) == 2

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            JSONSource(tmp_path / "missing.json").read()
