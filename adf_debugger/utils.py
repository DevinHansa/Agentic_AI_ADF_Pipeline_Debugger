"""
ADF Pipeline Debugger - Shared Utilities
"""
import logging
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure application logging."""
    logger = logging.getLogger("adf_debugger")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        mins = seconds / 60
        return f"{mins:.1f}m"
    else:
        hours = seconds / 3600
        mins = (seconds % 3600) / 60
        return f"{hours:.0f}h {mins:.0f}m"


def format_timestamp(dt: datetime) -> str:
    """Format datetime to human-readable string."""
    if dt is None:
        return "N/A"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def time_ago(dt: datetime) -> str:
    """Get human-readable time ago string."""
    if dt is None:
        return "N/A"
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    diff = now - dt
    total_seconds = diff.total_seconds()

    if total_seconds < 60:
        return "just now"
    elif total_seconds < 3600:
        mins = int(total_seconds / 60)
        return f"{mins} minute{'s' if mins != 1 else ''} ago"
    elif total_seconds < 86400:
        hours = int(total_seconds / 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    else:
        days = int(total_seconds / 86400)
        return f"{days} day{'s' if days != 1 else ''} ago"


def load_json(filepath: Path) -> dict:
    """Load and parse a JSON file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def truncate_string(s: str, max_length: int = 500) -> str:
    """Truncate a string with ellipsis if too long."""
    if not s:
        return ""
    if len(s) <= max_length:
        return s
    return s[:max_length] + "..."


def build_adf_portal_url(
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    pipeline_name: str = None,
    run_id: str = None,
) -> str:
    """Build a direct link to ADF in Azure Portal."""
    base = (
        f"https://adf.azure.com/en/monitoring/pipelineruns"
        f"?factory=/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.DataFactory/factories/{factory_name}"
    )
    if run_id:
        base += f"&runId={run_id}"
    return base


def severity_emoji(severity: str) -> str:
    """Get emoji for severity level."""
    return {
        "critical": "🔴",
        "high": "🟠",
        "medium": "🟡",
        "low": "🟢",
    }.get(severity.lower(), "⚪")


def error_category_emoji(category: str) -> str:
    """Get emoji for error category."""
    return {
        "connectivity": "🔌",
        "authentication": "🔐",
        "permission": "🚫",
        "data_quality": "📊",
        "timeout": "⏱️",
        "resource": "💾",
        "configuration": "⚙️",
        "schema": "📋",
        "missing_data": "📭",
        "quota": "📈",
        "unknown": "❓",
    }.get(category.lower(), "❓")
