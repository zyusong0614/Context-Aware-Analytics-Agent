import webbrowser

from rich.console import Console

from ca3_core.tracking import track_command
from ca3_core.ui import ask_confirm

console = Console()

DOCS_URL = "https://docs.getca3.io/"


@track_command("docs")
def docs():
    """Open ca3 official documentation in your browser"""
    console.print("\n[bold cyan] Ca3 Documentation[/bold cyan]\n")

    if ask_confirm("Open documentation in your browser?", default=True):
        webbrowser.open(DOCS_URL)
        console.print(f"[bold green]✓[/bold green] Opened {DOCS_URL}\n")
    else:
        console.print(f"[dim]Documentation available at: {DOCS_URL}\n[/dim]")
