from rich.console import Console
from rich.syntax import Syntax
from rich.text import Text


class InvalidPlaceholderError(Exception):
    def __init__(
        self,
        message: str,
        filename: str,
        line: int,
        column: int,
        source_line: str,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.filename = filename
        self.line = line
        self.column = column
        self.source_line = source_line

def print_friendly_error(error: Exception) -> None:
    console = Console()
    if isinstance(error, InvalidPlaceholderError):
        console.print(
            f"[bold red]Placeholder Error in {error.filename}:{error.line}[/bold red]"
        )
        syntax = Syntax(
            error.source_line.rstrip("\n"),
            "jinja",
            theme="monokai",
            line_numbers=False,
        )
        console.print(syntax)
        pointer = Text(" " * error.column + "^", style="bold red")
        console.print(pointer)
        console.print(f"[red]{error.message}[/red]")
    else:
        console.print_exception()
