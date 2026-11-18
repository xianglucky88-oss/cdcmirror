import pytest

def test_schema_drift_is_open_gap():
    from src.cdcmirror import schema_drift
    with pytest.raises(NotImplementedError):
        schema_drift.process({"id": "x"})
