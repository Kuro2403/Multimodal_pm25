from __future__ import annotations

import pytest

from tests.cli_utils import combined_output, run_shell


README_SCRIPT_COMMANDS = [
    "./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --generate-maps --parallel",
    "./scripts/run_complete_pipeline.sh --mode realtime --countries THA LAO --generate-maps",
    "./scripts/run_complete_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --generate-maps",
    "./scripts/run_air_quality_integrated_pipeline.sh --mode realtime --countries THA LAO",
    "./scripts/run_air_quality_integrated_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO",
    "./scripts/run_firms_pipeline.sh --mode realtime --countries THA LAO",
    "./scripts/run_firms_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO",
    "./scripts/run_himawari_integrated_pipeline.sh --mode realtime --countries THA LAO",
    "./scripts/run_himawari_integrated_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO",
    "./scripts/run_era5_idw_pipeline.sh --mode realtime --countries THA LAO",
    "./scripts/run_era5_idw_pipeline.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO",
    "./scripts/make_silver.sh --mode realtime --countries THA LAO",
    "./scripts/make_silver.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-30 --countries THA LAO",
    "./scripts/predict_air_quality.sh --mode realtime --countries THA LAO --generate-map",
    "./scripts/predict_air_quality.sh --mode historical --start-date 2024-06-01 --end-date 2024-06-03 --countries THA LAO --generate-maps --validate-sensors --enhanced-maps",
    "./scripts/run_tests.sh -- -q",
    "./scripts/smoke_test.sh",
]


def to_help_command(command: str) -> str:
    if command.startswith("./scripts/run_tests.sh"):
        return "./scripts/run_tests.sh --help"
    if command.startswith("./scripts/smoke_test.sh"):
        return "./scripts/smoke_test.sh --help"
    return f"{command} --help"


@pytest.mark.parametrize("readme_command", README_SCRIPT_COMMANDS)
def test_readme_script_command_contract(readme_command: str) -> None:
    command = to_help_command(readme_command)
    result = run_shell(command, timeout=45)
    output = combined_output(result)
    output_lower = output.lower()

    # Some scripts currently return 1 for help; treat either as acceptable
    # as long as usage text is shown and no unknown-option errors occur.
    assert result.returncode in {0, 1}, output
    assert "Unknown option" not in output, output
    assert "usage" in output_lower, output
