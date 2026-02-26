"""
ADF Pipeline Debugger - CLI Tool
Command-line interface for manual pipeline debugging and testing.
"""
import sys
import os
import io
import json
import click
from pathlib import Path

# Fix Windows Unicode encoding issue
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.markdown import Markdown
from rich import box

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import config
from adf_debugger.utils import (
    setup_logging, format_duration, format_timestamp,
    severity_emoji, error_category_emoji, build_adf_portal_url,
)
from adf_debugger.knowledge_base import KnowledgeBase
from adf_debugger.error_analyzer import ErrorAnalyzer
from adf_debugger.data_quality import DataQualityChecker
from adf_debugger.report_builder import ReportBuilder
from adf_debugger.notification import NotificationService

console = Console(force_terminal=True)
logger = setup_logging(config.app.LOG_LEVEL)


def _get_adf_client():
    """Initialize ADF client from config."""
    from adf_debugger.adf_client import ADFClient
    return ADFClient(
        subscription_id=config.azure.SUBSCRIPTION_ID,
        resource_group=config.azure.RESOURCE_GROUP,
        factory_name=config.azure.DATA_FACTORY_NAME,
        tenant_id=config.azure.TENANT_ID,
        client_id=config.azure.CLIENT_ID,
        client_secret=config.azure.CLIENT_SECRET,
    )


def _get_analyzer():
    """Initialize error analyzer from config."""
    return ErrorAnalyzer(
        api_key=config.gemini.API_KEY,
        model=config.gemini.MODEL,
    )


def _get_notification():
    """Initialize notification service from config."""
    return NotificationService(
        smtp_host=config.email.SMTP_HOST,
        smtp_port=config.email.SMTP_PORT,
        username=config.email.USERNAME,
        password=config.email.PASSWORD,
        from_address=config.email.FROM_ADDRESS,
    )


def _get_report_builder():
    """Initialize report builder from config."""
    return ReportBuilder(
        subscription_id=config.azure.SUBSCRIPTION_ID,
        resource_group=config.azure.RESOURCE_GROUP,
        factory_name=config.azure.DATA_FACTORY_NAME,
    )


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """🔧 ADF Pipeline Debugger - Intelligent Pipeline Triage Tool"""
    pass


@cli.command()
def test_connection():
    """Test connectivity to Azure Data Factory."""
    console.print("\n[bold cyan]🔌 Testing Azure Data Factory Connection...[/]")

    try:
        client = _get_adf_client()
        result = client.test_connection()

        if result["connected"]:
            console.print(Panel(
                f"[green]✅ Connected successfully![/]\n\n"
                f"[bold]Factory:[/] {result['factory_name']}\n"
                f"[bold]Location:[/] {result['location']}\n"
                f"[bold]State:[/] {result['provisioning_state']}\n"
                f"[bold]Pipelines:[/] {result['pipeline_count']}\n\n"
                f"[dim]Pipelines: {', '.join(result['pipelines'][:10])}"
                f"{'...' if len(result['pipelines']) > 10 else ''}[/]",
                title="Connection Test",
                border_style="green",
            ))
        else:
            console.print(Panel(
                f"[red]❌ Connection failed![/]\n\n"
                f"[bold]Error:[/] {result['error']}\n\n"
                f"[dim]Check your .env file and ensure credentials are correct.[/]",
                title="Connection Test",
                border_style="red",
            ))
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")


@cli.command()
@click.option("--hours", default=24, help="Hours to look back for failures")
def failures(hours):
    """List all recent pipeline failures."""
    console.print(f"\n[bold cyan]🔍 Scanning for failures in the last {hours} hours...[/]")

    try:
        client = _get_adf_client()
        runs = client.get_failed_pipeline_runs(hours_back=hours)

        if not runs:
            console.print("[green]✅ No pipeline failures found! All clear.[/]")
            return

        table = Table(
            title=f"Failed Pipeline Runs ({len(runs)} found)",
            box=box.ROUNDED,
            show_lines=True,
        )
        table.add_column("Pipeline", style="bold red")
        table.add_column("Run ID", style="dim")
        table.add_column("Start Time", style="cyan")
        table.add_column("Duration")
        table.add_column("Message", max_width=50)

        for run in runs:
            table.add_row(
                run.pipeline_name,
                run.run_id[:12] + "...",
                format_timestamp(run.run_start),
                format_duration((run.duration_in_ms or 0) / 1000),
                (run.message or "N/A")[:50],
            )

        console.print(table)
        console.print(f"\n[dim]Use 'python cli.py debug <run_id>' for detailed analysis[/]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/]")


@cli.command()
@click.argument("run_id")
@click.option("--send-email", is_flag=True, help="Send diagnostic email")
@click.option("--save-html", type=click.Path(), help="Save HTML report to file")
def debug(run_id, send_email, save_html):
    """Run full diagnostic analysis on a specific pipeline run."""
    console.print(f"\n[bold cyan]🔬 Analyzing pipeline run: {run_id}[/]")

    try:
        client = _get_adf_client()
        analyzer = _get_analyzer()
        quality_checker = DataQualityChecker(adf_client=client)
        report_builder = _get_report_builder()

        # Step 1: Get error details
        with console.status("[bold]Fetching error details from ADF..."):
            error_details = client.get_error_details(run_id)

        # Step 2: Get pipeline history
        with console.status("[bold]Fetching pipeline history..."):
            history = client.get_pipeline_history(
                error_details["pipeline_name"], count=5
            )

        # Step 3: Run data quality checks
        with console.status("[bold]Running data quality checks..."):
            quality_checks = quality_checker.run_checks(error_details)

        # Step 4: AI Analysis
        with console.status("[bold]Running AI analysis with Gemini..."):
            analysis = analyzer.analyze(error_details)

        # Display results
        _display_analysis(analysis, quality_checks)

        # Step 5: Build report
        report = report_builder.build_report(analysis, quality_checks, history)

        # Save HTML if requested
        if save_html:
            with open(save_html, "w", encoding="utf-8") as f:
                f.write(report["html"])
            console.print(f"\n[green]📄 HTML report saved to: {save_html}[/]")

        # Send email if requested
        if send_email:
            notifier = _get_notification()
            success = notifier.send_diagnostic_report(
                report=report,
                to_addresses=config.email.TO_ADDRESSES,
            )
            if success:
                console.print(f"\n[green]📧 Diagnostic email sent to: {', '.join(config.email.TO_ADDRESSES)}[/]")
            else:
                console.print("[red]❌ Failed to send email. Check SMTP settings.[/]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/]")
        raise


@cli.command()
@click.option("--send-email", is_flag=True, help="Send diagnostic email for the demo")
@click.option("--save-html", type=click.Path(), help="Save HTML report to file")
@click.option("--scenario", type=int, default=0, help="Scenario index (0-2)")
def demo(send_email, save_html, scenario):
    """Run a demo analysis with mock data (no Azure connection needed)."""
    console.print("\n[bold magenta]🎭 Running Demo Mode with Mock Data[/]")
    console.print("[dim]No Azure connection required for demo mode.\n[/]")

    from tests.mock_data.sample_failures import get_mock_pipeline_failures

    mock_failures = get_mock_pipeline_failures()
    if scenario >= len(mock_failures):
        console.print(f"[red]Invalid scenario. Choose 0-{len(mock_failures)-1}[/]")
        return

    error_details = mock_failures[scenario]

    # Initialize analyzer
    try:
        analyzer = _get_analyzer()
    except Exception:
        console.print("[yellow]⚠️ Gemini API not configured. Using knowledge base only.[/]")
        analyzer = None

    quality_checker = DataQualityChecker()
    report_builder = _get_report_builder()

    # Run analysis
    with console.status("[bold]Running data quality checks..."):
        quality_checks = quality_checker.run_checks(error_details)

    if analyzer:
        with console.status("[bold]Running AI analysis with Gemini..."):
            analysis = analyzer.analyze(error_details)
    else:
        # Use knowledge base only
        kb = KnowledgeBase()
        enrichment = kb.get_enrichment(error_details.get("primary_error_message", ""))
        analysis = {
            **error_details,
            "plain_english_error": f"Pipeline '{error_details['pipeline_name']}' failed at activity '{error_details['failing_activity_name']}': {error_details['primary_error_message'][:200]}",
            "root_cause": "AI analysis unavailable — using knowledge base match",
            "category": enrichment.get("category", "unknown"),
            "severity": enrichment.get("severity", "medium"),
            "solutions": [{"title": s, "steps": [], "estimated_time": "N/A", "likelihood": "medium"} for s in enrichment.get("known_solutions", [])],
            "known_solutions": enrichment.get("known_solutions", []),
            "runbook": enrichment.get("runbook"),
            "preventive_measures": ["Add retry policies", "Set up monitoring alerts"],
            "documentation_links": [{"title": "Docs", "url": u} for u in enrichment.get("documentation_links", [])],
            "additional_checks": [],
            "data_engineering_tips": "",
            "estimated_fix_time": enrichment.get("estimated_fix_time", "unknown"),
            "kb_pattern_matched": enrichment.get("pattern_matched", False),
        }

    # Display
    _display_analysis(analysis, quality_checks)

    # Build report
    report = report_builder.build_report(analysis, quality_checks)

    if save_html:
        with open(save_html, "w", encoding="utf-8") as f:
            f.write(report["html"])
        console.print(f"\n[green]📄 HTML report saved to: {save_html}[/]")

    if send_email:
        notifier = _get_notification()
        if config.email.TO_ADDRESSES:
            success = notifier.send_diagnostic_report(
                report=report,
                to_addresses=config.email.TO_ADDRESSES,
            )
            if success:
                console.print(f"\n[green]📧 Email sent to: {', '.join(config.email.TO_ADDRESSES)}[/]")
            else:
                console.print("[red]❌ Failed to send email[/]")
        else:
            console.print("[yellow]⚠️ No EMAIL_TO configured in .env[/]")


@cli.command()
def send_test_email():
    """Send a test email to verify SMTP configuration."""
    console.print("\n[bold cyan]📧 Sending test email...[/]")

    notifier = _get_notification()
    if config.email.TO_ADDRESSES:
        success = notifier.send_test_email(config.email.TO_ADDRESSES[0])
        if success:
            console.print(f"[green]✅ Test email sent to: {config.email.TO_ADDRESSES[0]}[/]")
        else:
            console.print("[red]❌ Failed to send test email. Check SMTP settings.[/]")
    else:
        console.print("[red]No EMAIL_TO configured in .env[/]")


@cli.command()
@click.argument("pipeline_name")
@click.option("--count", default=10, help="Number of runs to show")
def history(pipeline_name, count):
    """Show recent run history for a pipeline."""
    console.print(f"\n[bold cyan]📈 Pipeline History: {pipeline_name}[/]")

    try:
        client = _get_adf_client()
        runs = client.get_pipeline_history(pipeline_name, count=count)

        if not runs:
            console.print("[yellow]No runs found for this pipeline[/]")
            return

        table = Table(
            title=f"Recent Runs for {pipeline_name}",
            box=box.ROUNDED,
        )
        table.add_column("Status", justify="center")
        table.add_column("Start Time", style="cyan")
        table.add_column("Duration")
        table.add_column("Run ID", style="dim")

        for run in runs:
            status_icon = "✅" if run.status == "Succeeded" else "❌" if run.status == "Failed" else "⏳"
            status_style = "green" if run.status == "Succeeded" else "red" if run.status == "Failed" else "yellow"
            table.add_row(
                f"[{status_style}]{status_icon} {run.status}[/]",
                format_timestamp(run.run_start),
                format_duration((run.duration_in_ms or 0) / 1000),
                run.run_id[:12] + "...",
            )

        console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {e}[/]")


@cli.command()
@click.argument("error_message")
@click.option("--pipeline", default="Unknown", help="Pipeline name")
def analyze(error_message, pipeline):
    """Quick-analyze an error message (just paste the error)."""
    console.print(f"\n[bold cyan]🔬 Analyzing error message...[/]")

    try:
        analyzer = _get_analyzer()
        with console.status("[bold]Running AI analysis..."):
            result = analyzer.quick_analyze(error_message, pipeline_name=pipeline)
        _display_analysis(result)
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")
        # Fallback to knowledge base
        console.print("[yellow]Falling back to knowledge base...[/]")
        kb = KnowledgeBase()
        enrichment = kb.get_enrichment(error_message)
        if enrichment["pattern_matched"]:
            entry = enrichment["error_entry"]
            console.print(Panel(
                f"[bold]{entry['title']}[/]\n\n"
                f"{entry['description']}\n\n"
                f"[bold]Solutions:[/]\n" +
                "\n".join(f"  • {s}" for s in entry.get("solutions", [])),
                title=f"KB Match: {entry['id']}",
                border_style="yellow",
            ))
        else:
            console.print("[dim]No matching pattern found in knowledge base[/]")


def _display_analysis(analysis: dict, quality_checks: dict = None):
    """Display analysis results in the terminal using rich formatting."""
    severity = analysis.get("severity", "medium")
    severity_color = {
        "critical": "red", "high": "bright_red",
        "medium": "yellow", "low": "green"
    }.get(severity, "white")

    # Header
    console.print(Panel(
        f"[bold {severity_color}]{severity_emoji(severity)} {severity.upper()} SEVERITY[/]\n"
        f"[bold]Pipeline:[/] {analysis.get('pipeline_name', 'Unknown')}\n"
        f"[bold]Activity:[/] {analysis.get('failing_activity', 'Unknown')} ({analysis.get('failing_activity_type', '')})\n"
        f"[bold]Category:[/] {error_category_emoji(analysis.get('category', ''))} {analysis.get('category', 'unknown')}\n"
        f"[bold]Est. Fix Time:[/] {analysis.get('estimated_fix_time', 'unknown')}",
        title="Pipeline Failure Analysis",
        border_style=severity_color,
    ))

    # What went wrong
    console.print(Panel(
        analysis.get("plain_english_error", "No explanation available"),
        title="🔴 What Went Wrong",
        border_style="red",
    ))

    # Root cause
    console.print(Panel(
        analysis.get("root_cause", "Unable to determine"),
        title="🔍 Root Cause",
        border_style="yellow",
    ))

    # Solutions
    solutions = analysis.get("solutions", [])
    if solutions:
        for i, sol in enumerate(solutions, 1):
            steps_text = "\n".join(f"  → {s}" for s in sol.get("steps", []))
            console.print(Panel(
                f"⏱️ {sol.get('estimated_time', 'N/A')} | "
                f"Likelihood: {sol.get('likelihood', 'N/A')}\n\n"
                f"{steps_text}",
                title=f"💡 Solution {i}: {sol.get('title', '')}",
                border_style="green",
            ))

    # Quality checks
    if quality_checks:
        issues = quality_checks.get("issues_found", [])
        if issues:
            issues_text = "\n".join(f"  ⚠️ {i}" for i in issues)
            console.print(Panel(
                issues_text,
                title="📊 Data Quality Findings",
                border_style="cyan",
            ))

    # Preventive measures
    measures = analysis.get("preventive_measures", [])
    if measures:
        console.print(Panel(
            "\n".join(f"  ✓ {m}" for m in measures),
            title="🛡️ Preventive Measures",
            border_style="blue",
        ))


if __name__ == "__main__":
    cli()
