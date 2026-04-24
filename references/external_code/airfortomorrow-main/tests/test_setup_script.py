from __future__ import annotations

from tests.cli_utils import combined_output, run_shell


def test_setup_script_smoke() -> None:
    result = run_shell("./scripts/setup.sh", timeout=180)
    output = combined_output(result)

    assert result.returncode == 0, output
    assert "Runtime setup completed successfully!" in output, output
    assert "System is ready for air quality prediction pipeline" in output, output
