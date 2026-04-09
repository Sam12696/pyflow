"""Tests for built-in transforms."""
import pandas as pd
import pytest

from pyflow.transforms import FilterTransform, MapTransform, RenameTransform, CastTransform


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 17, 25],
        "salary": ["75000", "15000", "60000"],
    })


class TestFilterTransform:
    def test_callable_filter(self, sample_df):
        t = FilterTransform(lambda df: df["age"] >= 18)
        result = t.apply(sample_df)
        assert len(result) == 2
        assert "Bob" not in result["name"].values

    def test_query_string_filter(self, sample_df):
        t = FilterTransform("age >= 18")
        result = t.apply(sample_df)
        assert len(result) == 2

    def test_index_reset(self, sample_df):
        t = FilterTransform(lambda df: df["age"] > 20)
        result = t.apply(sample_df)
        assert list(result.index) == list(range(len(result)))


class TestMapTransform:
    def test_column_map(self, sample_df):
        t = MapTransform(str.upper, column="name")
        result = t.apply(sample_df)
        assert result["name"].iloc[0] == "ALICE"

    def test_dataframe_map(self, sample_df):
        t = MapTransform(lambda df: df.assign(senior=df["age"] > 28))
        result = t.apply(sample_df)
        assert "senior" in result.columns
        assert result["senior"].iloc[0] == True

    def test_original_not_mutated(self, sample_df):
        original_names = sample_df["name"].tolist()
        t = MapTransform(str.lower, column="name")
        t.apply(sample_df)
        assert sample_df["name"].tolist() == original_names


class TestRenameTransform:
    def test_rename(self, sample_df):
        t = RenameTransform({"name": "full_name", "salary": "annual_salary"})
        result = t.apply(sample_df)
        assert "full_name" in result.columns
        assert "annual_salary" in result.columns
        assert "name" not in result.columns

    def test_partial_rename(self, sample_df):
        t = RenameTransform({"age": "years"})
        result = t.apply(sample_df)
        assert "years" in result.columns
        assert "name" in result.columns  # untouched


class TestCastTransform:
    def test_cast_to_int(self, sample_df):
        t = CastTransform({"salary": int})
        result = t.apply(sample_df)
        assert result["salary"].dtype == int

    def test_cast_to_float(self, sample_df):
        t = CastTransform({"salary": float})
        result = t.apply(sample_df)
        assert result["salary"].dtype == float

    def test_cast_datetime(self):
        df = pd.DataFrame({"joined": ["2021-01-01", "2022-06-15"]})
        t = CastTransform({"joined": "datetime64[ns]"})
        result = t.apply(df)
        assert pd.api.types.is_datetime64_any_dtype(result["joined"])

    def test_missing_column_raises(self, sample_df):
        t = CastTransform({"nonexistent": int})
        with pytest.raises(KeyError, match="nonexistent"):
            t.apply(sample_df)

    def test_original_not_mutated(self, sample_df):
        original_dtype = sample_df["salary"].dtype
        t = CastTransform({"salary": float})
        t.apply(sample_df)
        assert sample_df["salary"].dtype == original_dtype
