import os
import sys
import tempfile
from pathlib import Path

# Point Prefect at a throwaway home before it is imported, so the suite never
# reads or migrates the developer's own ~/.prefect/prefect.db. Running the tests
# under a newer Prefect than the one installed locally will otherwise upgrade
# that shared database in place, after which the older Prefect cannot start its
# ephemeral server at all.
_PREFECT_HOME = Path(tempfile.gettempdir()) / "sleep-edf-prefect-test-home"
_PREFECT_HOME.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("PREFECT_HOME", str(_PREFECT_HOME))

import pytest  # noqa: E402
from prefect.settings import (  # noqa: E402
    PREFECT_API_KEY,
    PREFECT_API_URL,
    PREFECT_SERVER_ALLOW_EPHEMERAL_MODE,
    temporary_settings,
)

# The dashboard modules import each other as siblings, because Streamlit puts
# the script's own directory on sys.path. Mirror that for the tests.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "viz"))


@pytest.fixture(autouse=True)
def prefect_test_fixture():
    """
    Configure Prefect to use ephemeral test mode.
    This prevents tests from trying to contact a running API server.
    """
    with temporary_settings(
        {
            PREFECT_API_URL: None,
            PREFECT_API_KEY: None,
            PREFECT_SERVER_ALLOW_EPHEMERAL_MODE: True,
        }
    ):
        yield
