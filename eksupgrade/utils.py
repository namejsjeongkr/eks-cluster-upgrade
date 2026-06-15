"""Define module level utilities to be used across the EKS Upgrade package."""

import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field

import typer
from rich.table import Table


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


def _fmt_duration(seconds: float | None) -> str:
    """Format a duration in seconds as '45s', '1m02s', or '1h03m'."""
    if seconds is None:
        return "-"
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    if total < 3600:
        return f"{total // 60}m{total % 60:02d}s"
    return f"{total // 3600}h{(total % 3600) // 60:02d}m"


@dataclass
class PhaseRecord:
    """Timing for a single upgrade phase."""

    name: str
    start_wall: float
    start_mono: float
    end_mono: float | None = None
    status: str = "running"  # "running" | "completed" | "failed"

    @property
    def duration_s(self) -> float | None:
        """Return elapsed seconds, or None if still running."""
        return None if self.end_mono is None else self.end_mono - self.start_mono


@dataclass
class PhaseTimer:
    """Collect per-phase start/end/duration and render a summary table."""

    records: list[PhaseRecord] = field(default_factory=list)

    def start(self, name: str) -> PhaseRecord:
        """Start a new phase and register its record."""
        rec = PhaseRecord(name=name, start_wall=time.time(), start_mono=time.monotonic())
        self.records.append(rec)
        return rec

    def finish(self, rec: PhaseRecord, status: str = "completed") -> None:
        """Mark a phase as finished and echo its result."""
        rec.end_mono = time.monotonic()
        rec.status = status
        if status == "failed":
            echo_warning(f"{rec.name} — failed ({_fmt_duration(rec.duration_s)})")
        else:
            echo_success(f"{rec.name} — completed ({_fmt_duration(rec.duration_s)})")

    @contextmanager
    def phase(self, name: str):
        """Context manager that records a phase, finishing on success or failure."""
        rec = self.start(name)
        try:
            yield rec
        except BaseException:
            self.finish(rec, status="failed")
            raise
        else:
            self.finish(rec, status="completed")

    def summary_table(self) -> Table:
        """Build a Rich Table summarising all recorded phases."""
        table = Table("Phase", "Start", "End", "Duration", "Status", title="EKS Upgrade Timeline")
        for rec in self.records:
            start_str = time.strftime("%H:%M:%S", time.localtime(rec.start_wall))
            end_str = (
                time.strftime("%H:%M:%S", time.localtime(rec.start_wall + rec.duration_s))
                if rec.duration_s is not None
                else "-"
            )
            table.add_row(rec.name, start_str, end_str, _fmt_duration(rec.duration_s), rec.status)
        if self.records:
            first = min(r.start_wall for r in self.records)
            last_end = max((r.start_wall + (r.duration_s or 0)) for r in self.records)
            table.add_row(
                "TOTAL",
                time.strftime("%H:%M:%S", time.localtime(first)),
                time.strftime("%H:%M:%S", time.localtime(last_end)),
                _fmt_duration(last_end - first),
                "—",
            )
        return table
