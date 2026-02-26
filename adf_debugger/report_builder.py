"""
ADF Pipeline Debugger - Report Builder
Builds structured diagnostic reports combining all analysis results.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from .utils import (
    format_duration,
    format_timestamp,
    time_ago,
    build_adf_portal_url,
    severity_emoji,
    error_category_emoji,
)

logger = logging.getLogger("adf_debugger.report_builder")


class ReportBuilder:
    """Builds rich diagnostic reports from analysis results."""

    def __init__(
        self,
        templates_dir: Path = None,
        subscription_id: str = "",
        resource_group: str = "",
        factory_name: str = "",
    ):
        if templates_dir is None:
            templates_dir = Path(__file__).parent.parent / "templates"

        self.templates_dir = templates_dir
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name

        # Set up Jinja2
        self.env = Environment(
            loader=FileSystemLoader(str(templates_dir)),
            autoescape=True,
        )
        # Add custom filters
        self.env.filters["format_duration"] = lambda ms: format_duration(ms / 1000) if ms else "N/A"
        self.env.filters["format_timestamp"] = format_timestamp
        self.env.filters["time_ago"] = time_ago
        self.env.filters["severity_emoji"] = severity_emoji
        self.env.filters["category_emoji"] = error_category_emoji

    def build_report(self, analysis: dict, quality_checks: dict = None, pipeline_history: list = None) -> dict:
        """
        Build a complete diagnostic report.

        Args:
            analysis: Dict from ErrorAnalyzer.analyze()
            quality_checks: Dict from DataQualityChecker.run_checks()
            pipeline_history: List of recent pipeline runs

        Returns:
            Dict with 'html', 'plain_text', and 'subject' keys.
        """
        # Build the portal URL
        portal_url = build_adf_portal_url(
            subscription_id=self.subscription_id,
            resource_group=self.resource_group,
            factory_name=self.factory_name,
            pipeline_name=analysis.get("pipeline_name"),
            run_id=analysis.get("run_id"),
        )

        # Build template context
        context = {
            "analysis": analysis,
            "quality_checks": quality_checks or {},
            "pipeline_history": self._format_pipeline_history(pipeline_history),
            "portal_url": portal_url,
            "factory_name": self.factory_name,
            "resource_group": self.resource_group,
            "generated_at": datetime.now(timezone.utc),
            "severity_emoji": severity_emoji(analysis.get("severity", "medium")),
            "category_emoji": error_category_emoji(analysis.get("category", "unknown")),
        }

        # Render HTML
        html = self._render_html(context)

        # Build plain text
        plain_text = self._build_plain_text(context)

        # Build email subject
        severity = analysis.get("severity", "medium").upper()
        pipeline_name = analysis.get("pipeline_name", "Unknown")
        subject = f"[{severity}] ADF Pipeline Failed: {pipeline_name}"

        return {
            "html": html,
            "plain_text": plain_text,
            "subject": subject,
            "severity": analysis.get("severity", "medium"),
            "pipeline_name": pipeline_name,
        }

    def _render_html(self, context: dict) -> str:
        """Render the HTML email template."""
        try:
            template = self.env.get_template("diagnostic_email.html")
            return template.render(**context)
        except Exception as e:
            logger.error(f"Failed to render HTML template: {e}")
            return self._fallback_html(context)

    def _build_plain_text(self, context: dict) -> str:
        """Build plain text version of the report."""
        a = context["analysis"]
        lines = [
            "=" * 60,
            f"ADF PIPELINE FAILURE DIAGNOSTIC REPORT",
            "=" * 60,
            "",
            f"Pipeline: {a.get('pipeline_name', 'Unknown')}",
            f"Run ID:   {a.get('run_id', 'N/A')}",
            f"Severity: {a.get('severity', 'medium').upper()}",
            f"Category: {a.get('category', 'unknown')}",
            f"Time:     {format_timestamp(a.get('run_start'))}",
            "",
            "-" * 60,
            "WHAT WENT WRONG",
            "-" * 60,
            a.get("plain_english_error", "No explanation available"),
            "",
            "-" * 60,
            "ROOT CAUSE",
            "-" * 60,
            a.get("root_cause", "Unable to determine"),
            "",
            "-" * 60,
            "SUGGESTED SOLUTIONS",
            "-" * 60,
        ]

        for i, sol in enumerate(a.get("solutions", []), 1):
            lines.append(f"\n{i}. {sol.get('title', 'Solution')}")
            lines.append(f"   Estimated time: {sol.get('estimated_time', 'N/A')}")
            lines.append(f"   Likelihood: {sol.get('likelihood', 'N/A')}")
            for step in sol.get("steps", []):
                lines.append(f"   - {step}")

        if a.get("known_solutions"):
            lines.append("\n" + "-" * 60)
            lines.append("ADDITIONAL KNOWN SOLUTIONS")
            lines.append("-" * 60)
            for sol in a["known_solutions"]:
                lines.append(f"  - {sol}")

        if a.get("runbook"):
            lines.append("\n" + "-" * 60)
            lines.append(f"RUNBOOK: {a['runbook'].get('title', 'Resolution Steps')}")
            lines.append("-" * 60)
            for step in a["runbook"].get("steps", []):
                lines.append(f"  {step}")

        if a.get("preventive_measures"):
            lines.append("\n" + "-" * 60)
            lines.append("PREVENTIVE MEASURES")
            lines.append("-" * 60)
            for measure in a["preventive_measures"]:
                lines.append(f"  - {measure}")

        lines.extend([
            "",
            "-" * 60,
            "QUICK LINKS",
            "-" * 60,
            f"  Azure Portal: {context.get('portal_url', 'N/A')}",
            "",
            "=" * 60,
            f"Generated by ADF Pipeline Debugger at {format_timestamp(context.get('generated_at'))}",
            "=" * 60,
        ])

        return "\n".join(lines)

    def _format_pipeline_history(self, history: list) -> list:
        """Format pipeline run history for display."""
        if not history:
            return []

        formatted = []
        for run in history:
            formatted.append({
                "status": run.status if hasattr(run, "status") else "Unknown",
                "start": format_timestamp(run.run_start if hasattr(run, "run_start") else None),
                "duration": format_duration((run.duration_in_ms or 0) / 1000) if hasattr(run, "duration_in_ms") else "N/A",
                "status_icon": "✅" if getattr(run, "status", "") == "Succeeded" else "❌",
            })
        return formatted

    def _fallback_html(self, context: dict) -> str:
        """Simple HTML fallback if template rendering fails."""
        a = context["analysis"]
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
        <h1 style="color: #e74c3c;">⚠️ ADF Pipeline Failure</h1>
        <h2>{a.get('pipeline_name', 'Unknown Pipeline')}</h2>
        <p><strong>Severity:</strong> {a.get('severity', 'medium').upper()}</p>
        <p><strong>Error:</strong> {a.get('plain_english_error', 'No details')}</p>
        <p><strong>Root Cause:</strong> {a.get('root_cause', 'Unknown')}</p>
        <h3>Solutions:</h3>
        <ul>
        {''.join(f"<li>{s.get('title', '')}</li>" for s in a.get('solutions', []))}
        </ul>
        <p><a href="{context.get('portal_url', '#')}">Open in Azure Portal</a></p>
        </body>
        </html>
        """
