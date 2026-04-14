"""CLI runner for scrapers and enrichment."""

import logging
import typer
from rich.console import Console
from rich.table import Table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = typer.Typer()
console = Console()


@app.command()
def scrape_kofia(dry_run: bool = False):
    """Scrape KOFIA professional registry."""
    from src.scrapers.kofia import run
    console.print("[bold green]Starting KOFIA scrape...[/]")
    created, updated = run(dry_run=dry_run)
    if not dry_run:
        console.print(f"[green]Done — created: {created}, updated: {updated}[/]")


@app.command()
def enrich(batch: bool = False, limit: int = 50):
    """Enrich unenriched candidates with Claude."""
    if batch:
        from src.enrichment.claude_enricher import run_batch_enrichment
        console.print("[bold blue]Submitting batch enrichment...[/]")
        batch_id = run_batch_enrichment()
        console.print(f"[green]Batch complete: {batch_id}[/]")
    else:
        from src.enrichment.claude_enricher import run_enrichment
        console.print(f"[bold blue]Enriching up to {limit} candidates...[/]")
        run_enrichment(batch_size=limit)
        console.print("[green]Enrichment done.[/]")


@app.command()
def stats():
    """Show database statistics."""
    import sys; sys.path.insert(0, ".")
    from src.database.db import init_db, get_session
    from src.database.models import Candidate, CandidateStatus
    init_db()
    session = get_session()
    try:
        total = session.query(Candidate).count()
        enriched = session.query(Candidate).filter(Candidate.enriched_at.isnot(None)).count()
        table = Table(title="Talent Database Stats")
        table.add_column("Metric")
        table.add_column("Value", justify="right")
        table.add_row("Total candidates", str(total))
        table.add_row("Enriched", str(enriched))
        for s in CandidateStatus:
            count = session.query(Candidate).filter(Candidate.status == s).count()
            table.add_row(f"Status: {s.value}", str(count))
        console.print(table)
    finally:
        session.close()


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8000):
    """Start the FastAPI dashboard."""
    import uvicorn
    uvicorn.run("src.api.main:app", host=host, port=port, reload=True)


if __name__ == "__main__":
    app()
