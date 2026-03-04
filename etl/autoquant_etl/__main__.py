"""
AutoQuant ETL — CLI Entry Point
Usage:
    python -m autoquant_etl migrate           # Run pending DB migrations
    python -m autoquant_etl seed              # Seed dimension tables
    python -m autoquant_etl health            # Check DB + connector health
    python -m autoquant_etl status            # Full system status
    python -m autoquant_etl extract-daily     # Daily VAHAN extraction
    python -m autoquant_etl reconcile         # Monthly VAHAN vs FADA reconciliation
    python -m autoquant_etl estimate-revenue  # Quarterly revenue proxy calculation
    python -m autoquant_etl asp-calibrate     # Calibrate ASP from earnings data
    python -m autoquant_etl backfill          # Historical backfill
    python -m autoquant_etl monitor           # Pipeline health check
"""

from __future__ import annotations

import asyncio
import sys
from datetime import date
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich import print as rprint

from autoquant_etl.config import Settings
from autoquant_etl.utils.logging import configure_logging
from autoquant_etl.utils.database import get_pool, close_pool
from autoquant_etl.utils.migrations import run_migrations
from autoquant_etl.utils.seeder import run_seed
from autoquant_etl.orchestrator import run_daily_pipeline
from autoquant_etl.backfill import run_backfill
from autoquant_etl.monitor import run_monitor

app = typer.Typer(
    name="autoquant-etl",
    help="AutoQuant ETL — India Auto Registrations & Demand Dashboard",
    no_args_is_help=True,
)
console = Console()


def _get_settings() -> Settings:
    return Settings()


@app.command()
def migrate(
    dry_run: bool = typer.Option(False, "--dry-run", help="Show migrations without applying"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Apply pending DB schema migrations."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            result = await run_migrations(pool, dry_run=dry_run, verbose=verbose)
            if result.applied:
                console.print(f"[green]✓ Applied {len(result.applied)} migration(s):[/green]")
                for m in result.applied:
                    console.print(f"  - {m}")
            else:
                console.print("[dim]No pending migrations.[/dim]")
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@app.command()
def seed(
    force: bool = typer.Option(False, "--force", help="Re-run even if already seeded"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Seed dimension tables (OEM, segment, fuel, geo, vehicle class map)."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            await run_seed(pool, force=force, verbose=verbose)
            console.print("[green]✓ Dimension tables seeded.[/green]")
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@app.command()
def health() -> None:
    """Check database connectivity and connector health."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    async def _run():
        import asyncpg
        from autoquant_etl.connectors.vahan import VahanConnector

        console.print("\n[bold]AutoQuant ETL — Health Check[/bold]\n")

        # 1. DB connectivity
        try:
            pool = await get_pool(settings.database_url)
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
                row_count = await conn.fetchval("SELECT COUNT(*) FROM dim_oem")
            await close_pool(pool)
            console.print(f"[green]✓ Database connected.[/green] dim_oem rows: {row_count}")
        except Exception as exc:
            console.print(f"[red]✗ Database error: {exc}[/red]")
            raise typer.Exit(1)

        # 2. VAHAN connector ping
        try:
            async with VahanConnector(settings) as vc:
                ok = await vc.health_check()
            status = "[green]✓ VAHAN reachable[/green]" if ok else "[yellow]⚠ VAHAN unreachable[/yellow]"
            console.print(status)
        except Exception as exc:
            console.print(f"[yellow]⚠ VAHAN health check failed: {exc}[/yellow]")

        console.print()

    asyncio.run(_run())


@app.command()
def status() -> None:
    """Show full system status (pipeline, data freshness, recent runs)."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            async with pool.acquire() as conn:
                # Pipeline status
                rows = await conn.fetch("SELECT metric, value FROM v_pipeline_status")
                console.print("\n[bold]Pipeline Status[/bold]")
                t = Table(show_header=True)
                t.add_column("Metric")
                t.add_column("Value")
                for r in rows:
                    t.add_row(r["metric"], str(r["value"]))
                console.print(t)

                # Data freshness
                rows = await conn.fetch(
                    "SELECT source, last_attempted, last_success, failures_24h "
                    "FROM v_data_freshness ORDER BY source"
                )
                console.print("\n[bold]Data Freshness[/bold]")
                t2 = Table(show_header=True)
                t2.add_column("Source")
                t2.add_column("Last Attempted")
                t2.add_column("Last Success")
                t2.add_column("Failures 24h")
                for r in rows:
                    t2.add_row(
                        r["source"],
                        str(r["last_attempted"])[:19] if r["last_attempted"] else "never",
                        str(r["last_success"])[:19] if r["last_success"] else "never",
                        str(r["failures_24h"]),
                    )
                console.print(t2)

                # Unmapped makers
                unmapped = await conn.fetch(
                    "SELECT raw_maker_name, occurrence_count FROM v_unmapped_makers LIMIT 10"
                )
                if unmapped:
                    console.print("\n[yellow]Unmapped Makers (top 10):[/yellow]")
                    for r in unmapped:
                        console.print(f"  - {r['raw_maker_name']} ({r['occurrence_count']} occurrences)")
                else:
                    console.print("\n[green]✓ No unmapped makers.[/green]")
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@app.command(name="extract-daily")
def extract_daily(
    dry_run: bool = typer.Option(False, "--dry-run", help="Extract but don't write to DB"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    target_date: Optional[str] = typer.Option(
        None, "--date", help="Target date YYYY-MM-DD (default: yesterday)"
    ),
) -> None:
    """Run daily VAHAN extraction pipeline."""
    settings = _get_settings()
    if settings.dry_run:
        dry_run = True
    configure_logging(settings.log_level)

    parsed_date: Optional[date] = None
    if target_date:
        try:
            parsed_date = date.fromisoformat(target_date)
        except ValueError:
            console.print(f"[red]Invalid date format: {target_date}. Use YYYY-MM-DD.[/red]")
            raise typer.Exit(1)

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            result = await run_daily_pipeline(
                pool=pool,
                settings=settings,
                dry_run=dry_run,
                verbose=verbose,
                target_date=parsed_date,
            )
            if result.success:
                console.print(
                    f"[green]✓ Extraction complete.[/green] "
                    f"{result.records_extracted} extracted, "
                    f"{result.records_loaded} loaded."
                )
            else:
                console.print(f"[red]✗ Extraction failed: {result.error}[/red]")
                raise typer.Exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@app.command()
def reconcile(
    month: Optional[str] = typer.Option(
        None,
        "--month",
        help="Month to reconcile (YYYY-MM). Defaults to prior month.",
    ),
    pdf_path: Optional[str] = typer.Option(None, "--pdf-path", help="Local PDF path"),
    pdf_url: Optional[str] = typer.Option(None, "--pdf-url", help="URL of FADA PDF"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run monthly VAHAN vs FADA reconciliation."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    from autoquant_etl.transforms.reconcile import run_reconciliation
    from datetime import date
    import calendar

    if month:
        try:
            year, mon = map(int, month.split("-"))
            reconcile_month = date(year, mon, 1)
        except ValueError:
            console.print(f"[red]Invalid month format: {month}. Use YYYY-MM.[/red]")
            raise typer.Exit(1)
    else:
        today = date.today()
        first_of_month = date(today.year, today.month, 1)
        last_month = first_of_month - __import__('datetime').timedelta(days=1)
        reconcile_month = date(last_month.year, last_month.month, 1)

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            result = await run_reconciliation(
                pool=pool,
                settings=settings,
                report_month=reconcile_month,
                pdf_path=pdf_path,
                pdf_url=pdf_url,
                dry_run=dry_run,
                verbose=verbose,
            )
            if result.passed:
                console.print(f"[green]✓ Reconciliation passed.[/green] Delta: {result.total_delta_pct:.1f}%")
            else:
                console.print(f"[yellow]⚠ Reconciliation issues:[/yellow]")
                for issue in result.issues:
                    console.print(f"  - {issue}")
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@app.command(name="estimate-revenue")
def estimate_revenue(
    quarter: Optional[str] = typer.Option(
        None,
        "--quarter",
        help="FY Quarter e.g. Q3FY26. Defaults to current quarter.",
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Compute quarterly demand-based revenue proxy (registrations × ASP)."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    from autoquant_etl.transforms.gold import run_revenue_estimation

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            result = await run_revenue_estimation(
                pool=pool,
                settings=settings,
                quarter=quarter,
                dry_run=dry_run,
                verbose=verbose,
            )
            console.print(
                f"[green]✓ Revenue estimation complete.[/green] "
                f"{result.oem_count} OEMs, {result.quarter} processed."
            )
            if verbose and result.rows:
                t = Table(title=f"Revenue Proxy — {result.quarter}")
                t.add_column("OEM")
                t.add_column("Segment")
                t.add_column("Units")
                t.add_column("ASP (L)")
                t.add_column("Revenue Est (Cr)")
                for row in result.rows:
                    t.add_row(
                        row.oem_name,
                        row.segment_code,
                        f"{row.units_retail:,}",
                        f"{row.asp_used:.2f}",
                        f"{row.revenue_retail_cr:.1f}",
                    )
                console.print(t)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@app.command(name="asp-calibrate")
def asp_calibrate(
    oem: str = typer.Argument(help="OEM name as in dim_oem.oem_name"),
    segment: str = typer.Argument(help="Segment code: PV / CV / 2W"),
    asp_lakhs: float = typer.Argument(help="New ASP in INR Lakhs"),
    effective_from: Optional[str] = typer.Option(
        None, "--from", help="Effective from date YYYY-MM-DD (default: today)"
    ),
    source: str = typer.Option("EARNINGS_DISCLOSURE", "--source"),
    notes: str = typer.Option("", "--notes"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Calibrate ASP assumption from earnings data."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    from autoquant_etl.transforms.asp_manager import update_asp
    from datetime import date

    eff_date = date.today()
    if effective_from:
        try:
            eff_date = date.fromisoformat(effective_from)
        except ValueError:
            console.print(f"[red]Invalid date: {effective_from}[/red]")
            raise typer.Exit(1)

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            result = await update_asp(
                pool=pool,
                oem_name=oem,
                segment_code=segment,
                asp_inr_lakhs=asp_lakhs,
                effective_from=eff_date,
                source=source,
                notes=notes,
                dry_run=dry_run,
            )
            if dry_run:
                console.print(f"[dim]DRY RUN: Would set {oem} {segment} ASP to {asp_lakhs}L from {eff_date}[/dim]")
            else:
                console.print(
                    f"[green]✓ ASP updated.[/green] "
                    f"{oem} {segment}: {result.old_asp:.2f}L → {asp_lakhs:.2f}L"
                )
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@app.command()
def backfill(
    from_month: str = typer.Option(..., "--from-month", help="Start month YYYY-MM"),
    to_month: str = typer.Option(..., "--to-month", help="End month YYYY-MM"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    force: bool = typer.Option(False, "--force", help="Re-extract even if data exists"),
) -> None:
    """Run historical backfill for a date range."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            result = await run_backfill(
                pool=pool,
                settings=settings,
                from_month=from_month,
                to_month=to_month,
                dry_run=dry_run,
                verbose=verbose,
                force=force,
            )
            console.print(
                f"[green]✓ Backfill complete.[/green] "
                f"{result.months_processed} months, "
                f"{result.records_loaded} records loaded."
            )
        finally:
            await close_pool(pool)

    asyncio.run(_run())


@app.command()
def monitor(
    digest: bool = typer.Option(False, "--digest", help="Send Telegram digest"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run pipeline health monitor."""
    settings = _get_settings()
    configure_logging(settings.log_level)

    async def _run():
        pool = await get_pool(settings.database_url)
        try:
            result = await run_monitor(
                pool=pool,
                settings=settings,
                send_digest=digest,
                verbose=verbose,
            )
            if result.healthy:
                console.print("[green]✓ All health checks passed.[/green]")
            else:
                console.print(f"[yellow]⚠ {result.failed_checks} health check(s) failed.[/yellow]")
                for issue in result.issues:
                    console.print(f"  - {issue}")
                raise typer.Exit(1)
        finally:
            await close_pool(pool)

    asyncio.run(_run())


if __name__ == "__main__":
    app()
