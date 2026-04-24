import os
from pathlib import Path

import pytest


def _is_running_in_docker() -> bool:
    return os.getenv("AIR_QUALITY_TEST_IN_DOCKER") == "1" or Path("/.dockerenv").exists()


def pytest_sessionstart(session: pytest.Session) -> None:
    if os.getenv("AIR_QUALITY_ALLOW_HOST_TESTS") == "1":
        return
    if not _is_running_in_docker():
        raise pytest.UsageError(
            "Tests must run inside Docker. Use './scripts/run_tests.sh [-- <pytest args>]'. "
            "Set AIR_QUALITY_ALLOW_HOST_TESTS=1 only for local debugging."
        )
