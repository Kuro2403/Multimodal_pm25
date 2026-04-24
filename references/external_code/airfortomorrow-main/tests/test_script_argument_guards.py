from __future__ import annotations

import pytest

from tests.cli_utils import combined_output, run_shell


HISTORICAL_REQUIRED_DATE_COMMANDS = [
    (
        "./scripts/run_air_quality_integrated_pipeline.sh --mode historical --countries THA LAO",
        "Historical mode requires both --start-date and --end-date",
    ),
    (
        "./scripts/run_himawari_integrated_pipeline.sh --mode historical --countries THA LAO",
        "Historical mode requires both --start-date and --end-date",
    ),
    (
        "./scripts/run_era5_idw_pipeline.sh --mode historical --countries THA LAO",
        "Historical mode requires --start-date and --end-date",
    ),
    (
        "./scripts/make_silver.sh --mode historical --countries THA LAO",
        "Historical mode requires --start-date and --end-date",
    ),
]


@pytest.mark.parametrize(("command", "error_text"), HISTORICAL_REQUIRED_DATE_COMMANDS)
def test_historical_scripts_fail_fast_without_dates(command: str, error_text: str) -> None:
    result = run_shell(command, timeout=45)
    output = combined_output(result)

    assert result.returncode != 0, output
    assert error_text in output, output
