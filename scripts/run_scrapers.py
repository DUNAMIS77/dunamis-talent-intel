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
def scrape_wanted(talent: bool = True, jobs: bool = False, dry_run: bool = False):
    """Scrape Wanted (원티드) talent profiles and/or job postings."""
    from src.scrapers.wanted import run_talent, run_jobs
    if talent:
        console.print("[bold green]Scraping Wanted talent profiles...[/]")
        created, updated = run_talent(dry_run=dry_run)
        if not dry_run:
            console.print(f"[green]Done — created: {created}, updated: {updated}[/]")
    if jobs:
        console.print("[bold green]Scraping Wanted + Jumpit job postings...[/]")
        result = run_jobs(dry_run=dry_run)
        console.print(f"[green]Found {result['firms']} hiring firms, {result['postings']} postings[/]")


@app.command()
def scrape_linkedin(
    mode: str = typer.Argument("search", help="sweep | search | enrich"),
    budget: int = 200,
    dry_run: bool = False,
):
    """
    Scrape LinkedIn via Proxycurl API (requires PROXYCURL_API_KEY).

    Modes:\n
      sweep   — pull all investment staff at 18 target Korean firms (credit-heavy)\n
      search  — search by role keyword across Korea (lighter)\n
      enrich  — fetch full profiles for candidates already in DB with a LinkedIn URL
    """
    if mode == "sweep":
        from src.scrapers.linkedin import run_company_sweep
        console.print(f"[bold green]LinkedIn company sweep (budget: {budget} credits)...[/]")
        created, updated = run_company_sweep(credit_budget=budget, dry_run=dry_run)
        if not dry_run:
            console.print(f"[green]Done — created: {created}, updated: {updated}[/]")

    elif mode == "search":
        from src.scrapers.linkedin import run_person_search
        console.print(f"[bold green]LinkedIn person search (budget: {budget} credits)...[/]")
        created, updated = run_person_search(credit_budget=budget, dry_run=dry_run)
        if not dry_run:
            console.print(f"[green]Done — created: {created}, updated: {updated}[/]")

    elif mode == "enrich":
        from src.scrapers.linkedin import run_profile_enrich
        console.print(f"[bold green]LinkedIn profile enrich (budget: {budget} credits)...[/]")
        enriched = run_profile_enrich(credit_budget=budget, dry_run=dry_run)
        if not dry_run:
            console.print(f"[green]Done — enriched: {enriched} profiles[/]")

    else:
        console.print(f"[red]Unknown mode '{mode}'. Use: sweep | search | enrich[/]")
        raise typer.Exit(1)


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
