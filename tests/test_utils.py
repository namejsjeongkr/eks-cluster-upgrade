"""Test the util logic."""

from typer.testing import CliRunner

from eksupgrade.utils import confirm, echo_deprecation, echo_error, echo_info, echo_success, echo_warning

runner = CliRunner()


def test_echo_deprecation(app) -> None:
    """Test the echo deprecation method."""
    app.command()(echo_deprecation)
    result = runner.invoke(app, ["this is a deprecation"])
    assert "this is a deprecation" in result.stdout
    assert result.exit_code == 0


def test_echo_error(app) -> None:
    """Test the echo error method."""
    app.command()(echo_error)
    result = runner.invoke(app, ["this is a error"])
    # echo_error writes to stderr (err=True); result.output captures both streams.
    assert "this is a error" in result.output
    assert result.exit_code == 0


def test_echo_info(app) -> None:
    """Test the echo info method."""
    app.command()(echo_info)
    result = runner.invoke(app, ["this is a info"])
    assert "this is a info" in result.stdout
    assert result.exit_code == 0


def test_echo_success(app) -> None:
    """Test the echo success method."""
    app.command()(echo_success)
    result = runner.invoke(app, ["this is a success"])
    assert "this is a success" in result.stdout
    assert result.exit_code == 0


def test_echo_warning(app) -> None:
    """Test the echo warning method."""
    app.command()(echo_warning)
    result = runner.invoke(app, ["this is a warning"])
    assert "this is a warning" in result.stdout
    assert result.exit_code == 0


def test_confirm_yes(app) -> None:
    """Test the confirm method with input y for yes."""
    app.command()(confirm)
    result = runner.invoke(app, ["this is a confirmation prompt"], input="y\n")
    assert "this is a confirmation prompt" in result.stdout
    assert result.exit_code == 0


def test_confirm_no(app) -> None:
    """Test the confirm method with input n for no."""
    app.command()(confirm)
    result = runner.invoke(app, ["this is a confirmation prompt"], input="n\n")
    assert "this is a confirmation prompt" in result.stdout
    assert result.exit_code == 1


def test_confirm_no_without_abort(app) -> None:
    """Test the confirm method with input n for no and abort disabled."""
    app.command()(confirm)
    result = runner.invoke(app, ["this is a confirmation prompt", "--no-abort"], input="n\n")
    assert "this is a confirmation prompt" in result.stdout
    assert result.exit_code == 0
