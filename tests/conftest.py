import pytest
from prefect.settings import (
    PREFECT_API_URL,
    PREFECT_API_KEY,
    PREFECT_SERVER_ALLOW_EPHEMERAL_MODE,
    temporary_settings,
)

@pytest.fixture(autouse=True)
def prefect_test_fixture():
    """
    Configure Prefect to use ephemeral test mode.
    This prevents tests from trying to contact a running API server.
    """
    with temporary_settings({
        PREFECT_API_URL: None,
        PREFECT_API_KEY: None,
        PREFECT_SERVER_ALLOW_EPHEMERAL_MODE: True,
    }):
        yield
