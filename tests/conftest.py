"""Pytest configuration and shared fixtures."""
import pytest
import pandas as pd


@pytest.fixture(scope="session")
def sample_hl7_message():
    return (
        "MSH|^~\\&|EPIC|HOSPITAL|RECEIVER|DEST|20230615103000||ADT^A01|MSG001|P|2.5\n"
        "PID|||P001^^^HOSPITAL||Smith^John^A||19850320|M|||123 Main St^^Dallas^TX^75001"
    )


@pytest.fixture(scope="session")
def sample_patient_df():
    return pd.DataFrame([
        {"patient_id": "P001", "message_type": "ADT_A01", "message_timestamp": "2023-06-15T10:30:00", "gender": "M"},
        {"patient_id": "P002", "message_type": "ORU_R01", "message_timestamp": "2023-07-22T14:15:00", "gender": "F"},
    ])
