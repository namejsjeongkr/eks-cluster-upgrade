"""Define module level utilities to be used across the EKS Upgrade package."""

import logging
import sys

import typer


def get_logger(logger_name):
    """Get a logger object with handler set to StreamHandler."""
    logger = logging.getLogger(logger_name)
    console_handler = logging.StreamHandler(sys.stdout)
    log_formatter = logging.Formatter(
        "[%(levelname)s] : %(asctime)s : %(name)s.%(lineno)d : %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


def confirm(message: str, abort: bool = True) -> bool:
    """Prompt the user with a confirmation dialog with the provided message.

    Raises:
        typer.Abort: The exception is raised when abort=True and confirmation fails.

    Returns:
        bool: Whether or not the prompt was confirmed.

    """
    text = typer.style(message, fg=typer.colors.BRIGHT_BLUE, bold=True, bg=typer.colors.WHITE)
    return typer.confirm(text, abort=abort)


def echo_deprecation(message: str) -> None:
    """Echo a message as a deprecation notice."""
    typer.secho(message, fg=typer.colors.WHITE, bg=typer.colors.YELLOW, bold=True, blink=True)


def echo_error(message: str) -> None:
    """Echo a message as an error."""
    typer.secho(message, fg=typer.colors.WHITE, bg=typer.colors.RED, bold=True, blink=True, err=True)


def echo_success(message: str) -> None:
    """Echo a message as an error."""
    typer.secho(message, fg=typer.colors.WHITE, bg=typer.colors.GREEN, bold=True, blink=True)


def echo_info(message: str) -> None:
    """Echo a message as an error."""
    typer.secho(message, fg=typer.colors.BRIGHT_BLUE)


def echo_warning(message: str) -> None:
    """Echo a message as an error."""
    typer.secho(message, fg=typer.colors.BRIGHT_YELLOW, bold=True, blink=True)
