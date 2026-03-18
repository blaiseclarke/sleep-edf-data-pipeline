import pytest
from unittest.mock import patch
from pipeline import run_ingestion_pipeline


def test_invalid_subject_range_raises():
    """Verifies that STARTING_SUBJECT > ENDING_SUBJECT raises ValueError."""
    with patch("pipeline.STARTING_SUBJECT", 10), patch("pipeline.ENDING_SUBJECT", 5):
        with pytest.raises(ValueError, match="must be <="):
            run_ingestion_pipeline()
