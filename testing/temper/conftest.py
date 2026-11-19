import pytest

import harness


@pytest.fixture(scope="session", autouse=True)
def feldera():
    """Starts Feldera (open-source edition) unless it is already running."""
    harness.start_feldera()
