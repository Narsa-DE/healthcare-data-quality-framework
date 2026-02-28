"""Unit tests for data quality checking modules."""
import pytest
import pandas as pd
from src.quality.completeness_checker import CompletenessChecker


@pytest.fixture
def checker():
    return CompletenessChecker(threshold=0.99)


@pytest.fixture
def valid_df():
    return pd.DataFrame([
        {"patient_id": "P001", "message_type": "ADT_A01", "message_timestamp": "2023-06-15T10:30:00", "gender": "M"},
        {"patient_id": "P002", "message_type": "ADT_A01", "message_timestamp": "2023-06-15T10:31:00", "gender": "F"},
        {"patient_id": "P003", "message_type": "ORU_R01", "message_timestamp": "2023-06-15T10:32:00", "gender": "F"},
    ])


@pytest.fixture
def invalid_df():
    return pd.DataFrame([
        {"patient_id": None, "message_type": "ADT_A01", "message_timestamp": "2023-06-15T10:30:00", "gender": "M"},
        {"patient_id": "P002", "message_type": None, "message_timestamp": None, "gender": "F"},
        {"patient_id": "P003", "message_type": "ORU_R01", "message_timestamp": "2023-06-15T10:32:00", "gender": "F"},
    ])


class TestCompletenessChecker:
    def test_valid_records_pass(self, checker, valid_df):
        result = checker.validate_dataframe(valid_df)
        assert result.passed is True
        assert result.completeness_score == 1.0

    def test_invalid_records_fail(self, checker, invalid_df):
        result = checker.validate_dataframe(invalid_df)
        assert result.passed is False
        assert result.invalid_records > 0

    def test_single_record_valid(self, checker):
        record = {"patient_id": "P001", "message_type": "ADT_A01", "message_timestamp": "2023-06-15", "gender": "M"}
        is_valid, missing = checker.check_record(record)
        assert is_valid is True
        assert missing == []

    def test_single_record_missing_fields(self, checker):
        record = {"patient_id": "P001", "gender": "M"}
        is_valid, missing = checker.check_record(record)
        assert is_valid is False
        assert "message_type" in missing

    def test_threshold_enforcement(self):
        strict_checker = CompletenessChecker(threshold=1.0)
        df = pd.DataFrame([
            {"patient_id": "P001", "message_type": "ADT", "message_timestamp": "2023-01-01", "gender": "M"},
            {"patient_id": None, "message_type": "ADT", "message_timestamp": "2023-01-01", "gender": "F"},
        ])
        result = strict_checker.validate_dataframe(df)
        assert result.passed is False
