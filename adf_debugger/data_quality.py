"""
ADF Pipeline Debugger - Data Quality Checker
Performs data quality and availability checks related to pipeline failures.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("adf_debugger.data_quality")


class DataQualityChecker:
    """
    Checks data quality aspects that may be related to pipeline failures.
    Uses the ADF client to inspect linked services and data sources.
    """

    def __init__(self, adf_client=None):
        self.adf_client = adf_client

    def run_checks(self, error_details: dict) -> dict:
        """
        Run all applicable data quality checks based on the error context.
        Returns a summary of checks performed and their results.
        """
        checks = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_name": error_details.get("pipeline_name", "Unknown"),
            "checks_performed": [],
            "issues_found": [],
            "recommendations": [],
        }

        # Check 1: Timing analysis
        timing = self._check_timing(error_details)
        checks["checks_performed"].append(timing)
        if timing.get("issues"):
            checks["issues_found"].extend(timing["issues"])

        # Check 2: Activity success rate
        activity_check = self._check_activity_success_rate(error_details)
        checks["checks_performed"].append(activity_check)
        if activity_check.get("issues"):
            checks["issues_found"].extend(activity_check["issues"])

        # Check 3: Parameter validation
        param_check = self._check_parameters(error_details)
        checks["checks_performed"].append(param_check)
        if param_check.get("issues"):
            checks["issues_found"].extend(param_check["issues"])

        # Check 4: Failure pattern analysis
        pattern_check = self._check_failure_patterns(error_details)
        checks["checks_performed"].append(pattern_check)
        if pattern_check.get("issues"):
            checks["issues_found"].extend(pattern_check["issues"])

        # Generate recommendations
        checks["recommendations"] = self._generate_recommendations(checks)

        return checks

    def _check_timing(self, error_details: dict) -> dict:
        """Analyze timing aspects of the failure."""
        check = {
            "name": "Timing Analysis",
            "status": "passed",
            "details": {},
            "issues": [],
        }

        run_start = error_details.get("run_start")
        run_end = error_details.get("run_end")
        duration_ms = error_details.get("duration_ms")

        if run_start:
            # Check if failure happened during off-hours
            if isinstance(run_start, datetime):
                hour = run_start.hour
                if hour < 6 or hour > 22:
                    check["details"]["off_hours"] = True
                    check["issues"].append(
                        "Pipeline failed during off-hours — consider if source "
                        "systems have maintenance windows during this time"
                    )

                # Check day of week
                day = run_start.weekday()
                if day >= 5:  # Saturday or Sunday
                    check["details"]["weekend"] = True
                    check["issues"].append(
                        "Pipeline failed on a weekend — source systems may "
                        "have reduced capacity or scheduled maintenance"
                    )

        if duration_ms:
            check["details"]["duration_ms"] = duration_ms
            # Check if it failed very quickly (likely config/auth issue)
            if duration_ms < 5000:
                check["issues"].append(
                    "Pipeline failed within 5 seconds — likely a "
                    "connection, authentication, or configuration issue "
                    "rather than a data processing problem"
                )
            # Check if it ran very long before failing (likely timeout/resource)
            elif duration_ms > 3600000:  # More than 1 hour
                check["issues"].append(
                    "Pipeline ran for over 1 hour before failing — "
                    "likely a timeout, resource exhaustion, or "
                    "performance degradation issue"
                )

        if check["issues"]:
            check["status"] = "warning"

        return check

    def _check_activity_success_rate(self, error_details: dict) -> dict:
        """Analyze activity success/failure rates within the pipeline."""
        check = {
            "name": "Activity Success Rate",
            "status": "passed",
            "details": {},
            "issues": [],
        }

        total = error_details.get("total_activities", 0)
        failed = len(error_details.get("failed_activities", []))
        succeeded = len(error_details.get("succeeded_activities", []))

        check["details"] = {
            "total_activities": total,
            "succeeded": succeeded,
            "failed": failed,
        }

        if total > 0:
            if failed > 1:
                check["issues"].append(
                    f"Multiple activities failed ({failed}/{total}) — "
                    "this suggests a systemic issue rather than an "
                    "isolated activity problem"
                )
                check["status"] = "warning"
            elif succeeded > 0 and failed == 1:
                check["details"]["partial_success"] = True
                # The pipeline partially succeeded
                failing_activity = error_details.get("failing_activity_name", "Unknown")
                check["issues"].append(
                    f"Only activity '{failing_activity}' failed while "
                    f"{succeeded} other activities succeeded — the issue "
                    "is isolated to this specific activity"
                )

        return check

    def _check_parameters(self, error_details: dict) -> dict:
        """Validate pipeline parameters for common issues."""
        check = {
            "name": "Parameter Validation",
            "status": "passed",
            "details": {},
            "issues": [],
        }

        params = error_details.get("parameters", {})
        check["details"]["parameter_count"] = len(params)

        for key, value in params.items():
            # Check for empty parameters
            if value is None or (isinstance(value, str) and not value.strip()):
                check["issues"].append(
                    f"Parameter '{key}' is empty or null — this may "
                    "cause downstream activity failures"
                )

            # Check for placeholder values
            if isinstance(value, str):
                placeholders = ["TODO", "CHANGEME", "xxx", "placeholder", "example"]
                if any(p.lower() in value.lower() for p in placeholders):
                    check["issues"].append(
                        f"Parameter '{key}' contains a placeholder value: "
                        f"'{value}' — this should be replaced with actual data"
                    )

        if check["issues"]:
            check["status"] = "warning"

        return check

    def _check_failure_patterns(self, error_details: dict) -> dict:
        """Analyze the error for common failure patterns."""
        check = {
            "name": "Failure Pattern Analysis",
            "status": "passed",
            "details": {},
            "issues": [],
        }

        error_msg = error_details.get("primary_error_message", "").lower()
        failure_type = error_details.get("primary_failure_type", "").lower()

        # Check for transient errors
        transient_patterns = [
            "transient", "retry", "temporary", "intermittent",
            "service unavailable", "503", "429", "throttl"
        ]
        if any(p in error_msg for p in transient_patterns):
            check["details"]["likely_transient"] = True
            check["issues"].append(
                "This appears to be a transient error — it may resolve "
                "on its own. Consider adding retry policies to the activity."
            )

        # Check for user errors vs system errors
        if "usererror" in failure_type:
            check["details"]["error_type"] = "user_error"
            check["issues"].append(
                "This is classified as a UserError — it's likely caused by "
                "configuration, data, or permissions rather than a system issue."
            )
        elif "systemerror" in failure_type:
            check["details"]["error_type"] = "system_error"
            check["issues"].append(
                "This is classified as a SystemError — it may be an Azure "
                "platform issue. Check Azure Service Health dashboard."
            )

        if check["issues"]:
            check["status"] = "info"

        return check

    def _generate_recommendations(self, checks: dict) -> list:
        """Generate recommendations based on all check results."""
        recommendations = []
        issues = checks.get("issues_found", [])

        if any("off-hours" in i or "weekend" in i for i in issues):
            recommendations.append(
                "Set up monitoring to alert on-call engineers immediately "
                "when pipelines fail during off-hours"
            )

        if any("5 seconds" in i for i in issues):
            recommendations.append(
                "Add a pre-flight check activity (e.g., Lookup or Web activity) "
                "to validate connectivity before running the main pipeline"
            )

        if any("transient" in i for i in issues):
            recommendations.append(
                "Configure retry policies: set retryCount=3 with "
                "retryIntervalInSeconds=30 on the failing activity"
            )

        if any("parameter" in i.lower() for i in issues):
            recommendations.append(
                "Add parameter validation logic at the start of the pipeline "
                "using If Condition activities"
            )

        if not recommendations:
            recommendations.append(
                "Add comprehensive monitoring and alerting for this pipeline "
                "to catch failures early"
            )

        return recommendations
