"""
Rich-based terminal UI: mission tables, brain thoughts, progress.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Sequence, Tuple

_console = None


def get_console():
    global _console
    if _console is None:
        from rich.console import Console

        _console = Console()
    return _console


def log_brain_thought(message: str, *, style: str = "cyan") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    c = get_console()
    c.print(f"[dim]{ts}[/dim] [bold {style}]Local Brain[/bold {style}] → {message}")


def print_mission_table(title: str, rows: Sequence[Tuple[str, str, str]]) -> None:
    from rich.table import Table

    table = Table(title=title, show_header=True, header_style="bold magenta")
    table.add_column("#", style="dim", width=4)
    table.add_column("Phase", style="white")
    table.add_column("Status", style="green")
    for step, phase, status in rows:
        table.add_row(step, phase, status)
    get_console().print(table)


def run_mission_progress(description: str, total: int, items: Iterable[str]) -> None:
    from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

    item_list: List[str] = list(items)
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=get_console(),
        transient=False,
    ) as progress:
        task = progress.add_task(description, total=max(total, 1))
        for i, thought in enumerate(item_list):
            log_brain_thought(thought, style="green")
            progress.update(task, advance=1, description=f"{description} ({thought[:48]}…)" if len(thought) > 48 else f"{description} — {thought}")
        progress.update(task, completed=total)
