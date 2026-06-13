"""Test the functionality of the CLI module."""

from typer.testing import CliRunner

from eksupgrade.cli import app

runner = CliRunner()


def test_entry_version_arg() -> None:
    """Test the entry method with version argument."""
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "eksupgrade version" in result.stdout


def test_entry_no_arg() -> None:
    """Test the entry method with no arguments."""
    result = runner.invoke(app, [])
    assert result.exit_code == 2
    # Newer Click routes usage errors to stderr; result.output captures both streams.
    assert "OPTIONS" in result.output
