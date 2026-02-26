"""
ADF Pipeline Debugger - Azure Function
Event-driven pipeline failure monitoring triggered by Azure Monitor alerts
or timer-based scanning.
"""
import sys
import os
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

import azure.functions as func

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from adf_debugger.adf_client import ADFClient
from adf_debugger.error_analyzer import ErrorAnalyzer
from adf_debugger.data_quality import DataQualityChecker
from adf_debugger.report_builder import ReportBuilder
from adf_debugger.notification import NotificationService

app = func.FunctionApp()


def _run_analysis_and_notify(run_id: str = None, pipeline_name: str = None):
    """Core logic: fetch error details, analyze, and send email."""
    logging.info(f"Processing failure analysis for run_id={run_id}, pipeline={pipeline_name}")

    # Initialize services
    adf_client = ADFClient(
        subscription_id=config.azure.SUBSCRIPTION_ID,
        resource_group=config.azure.RESOURCE_GROUP,
        factory_name=config.azure.DATA_FACTORY_NAME,
        tenant_id=config.azure.TENANT_ID,
        client_id=config.azure.CLIENT_ID,
        client_secret=config.azure.CLIENT_SECRET,
    )
    analyzer = ErrorAnalyzer(api_key=config.gemini.API_KEY, model=config.gemini.MODEL)
    quality_checker = DataQualityChecker(adf_client=adf_client)
    report_builder = ReportBuilder(
        subscription_id=config.azure.SUBSCRIPTION_ID,
        resource_group=config.azure.RESOURCE_GROUP,
        factory_name=config.azure.DATA_FACTORY_NAME,
    )
    notifier = NotificationService(
        smtp_host=config.email.SMTP_HOST,
        smtp_port=config.email.SMTP_PORT,
        username=config.email.USERNAME,
        password=config.email.PASSWORD,
        from_address=config.email.FROM_ADDRESS,
    )

    if run_id:
        # Analyze specific run
        error_details = adf_client.get_error_details(run_id)
        analysis = analyzer.analyze(error_details)
        quality_checks = quality_checker.run_checks(error_details)
        history = adf_client.get_pipeline_history(error_details["pipeline_name"], count=5)
        report = report_builder.build_report(analysis, quality_checks, history)

        notifier.send_diagnostic_report(
            report=report,
            to_addresses=config.email.TO_ADDRESSES,
        )
    else:
        # Scan for all recent failures
        failures = adf_client.get_failed_pipeline_runs(hours_back=config.app.LOOKBACK_HOURS)
        logging.info(f"Found {len(failures)} failures to process")

        for run in failures:
            try:
                error_details = adf_client.get_error_details(run.run_id)
                analysis = analyzer.analyze(error_details)
                quality_checks = quality_checker.run_checks(error_details)
                history = adf_client.get_pipeline_history(run.pipeline_name, count=5)
                report = report_builder.build_report(analysis, quality_checks, history)

                notifier.send_diagnostic_report(
                    report=report,
                    to_addresses=config.email.TO_ADDRESSES,
                )
            except Exception as e:
                logging.error(f"Failed to process run {run.run_id}: {e}")


# ===== Azure Monitor Alert Trigger =====
@app.function_name(name="AdfAlertTrigger")
@app.route(route="adf-alert", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def adf_alert_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    Triggered by Azure Monitor Alert webhook when a pipeline fails.
    Receives the alert payload, extracts the run ID, and runs analysis.
    """
    logging.info("ADF Alert webhook triggered")

    try:
        body = req.get_json()

        # Extract run ID from the alert payload
        # Azure Monitor alerts have different schemas, we handle common ones
        run_id = None
        pipeline_name = None

        # Try Common Alert Schema
        alert_data = body.get("data", {})
        essentials = alert_data.get("essentials", {})
        alert_context = alert_data.get("alertContext", {})

        # Try to get from custom properties or dimensions
        custom_props = alert_context.get("properties", {})
        run_id = custom_props.get("runId") or custom_props.get("run_id")
        pipeline_name = custom_props.get("pipelineName") or custom_props.get("pipeline_name")

        # If not in properties, try condition dimensions
        condition = alert_context.get("condition", {})
        for allOf in condition.get("allOf", []):
            for dim in allOf.get("dimensions", []):
                if dim.get("name") == "RunId":
                    run_id = dim.get("value")
                elif dim.get("name") == "PipelineName":
                    pipeline_name = dim.get("value")

        _run_analysis_and_notify(run_id=run_id, pipeline_name=pipeline_name)

        return func.HttpResponse(
            json.dumps({"status": "processed", "run_id": run_id}),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"Alert processing failed: {e}")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            mimetype="application/json",
            status_code=500,
        )


# ===== Timer Trigger (runs every 30 minutes) =====
@app.function_name(name="AdfFailureScanner")
@app.timer_trigger(schedule="0 */30 * * * *", arg_name="timer", run_on_startup=False)
def adf_failure_scanner(timer: func.TimerRequest):
    """
    Periodically scan for pipeline failures and send diagnostic emails.
    Runs every 30 minutes by default.
    """
    logging.info("ADF Failure Scanner triggered (timer)")
    _run_analysis_and_notify()
    logging.info("ADF Failure Scanner completed")
