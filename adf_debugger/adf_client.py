"""
ADF Pipeline Debugger - Azure Data Factory Client
Wraps the Azure SDK to fetch pipeline run details, errors, and activity information.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters, RunQueryFilter, RunQueryFilterOperand, RunQueryFilterOperator

logger = logging.getLogger("adf_debugger.adf_client")


class ADFClient:
    """Client for interacting with Azure Data Factory."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        factory_name: str,
        tenant_id: str = None,
        client_id: str = None,
        client_secret: str = None,
    ):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name

        # Authenticate
        if tenant_id and client_id and client_secret:
            self.credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
            logger.info("Authenticated with Service Principal")
        else:
            self.credential = DefaultAzureCredential()
            logger.info("Authenticated with Default Azure Credential")

        self.client = DataFactoryManagementClient(
            credential=self.credential,
            subscription_id=self.subscription_id,
        )
        logger.info(
            f"ADF Client initialized for {factory_name} "
            f"in {resource_group}"
        )

    def get_failed_pipeline_runs(self, hours_back: int = 24) -> list:
        """
        Fetch all failed pipeline runs in the last N hours.
        Returns a list of pipeline run objects.
        """
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=hours_back)

        filter_params = RunFilterParameters(
            last_updated_after=start_time,
            last_updated_before=now,
            filters=[
                RunQueryFilter(
                    operand=RunQueryFilterOperand.STATUS,
                    operator=RunQueryFilterOperator.EQUALS,
                    values=["Failed"],
                )
            ],
        )

        try:
            result = self.client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters=filter_params,
            )
            runs = list(result.value) if result.value else []
            logger.info(
                f"Found {len(runs)} failed pipeline run(s) "
                f"in the last {hours_back} hours"
            )
            return runs
        except Exception as e:
            logger.error(f"Failed to query pipeline runs: {e}")
            raise

    def get_pipeline_run_details(self, run_id: str) -> dict:
        """
        Get full details for a specific pipeline run.
        Returns a dict with all run metadata.
        """
        try:
            run = self.client.pipeline_runs.get(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                run_id=run_id,
            )
            return {
                "run_id": run.run_id,
                "pipeline_name": run.pipeline_name,
                "status": run.status,
                "message": getattr(run, "message", None),
                "run_start": run.run_start,
                "run_end": run.run_end,
                "duration_ms": run.duration_in_ms,
                "parameters": run.parameters or {},
                "invoked_by": {
                    "name": getattr(run.invoked_by, "name", "Unknown"),
                    "type": getattr(run.invoked_by, "invoked_by_type", "Unknown"),
                } if run.invoked_by else {},
                "run_group_id": run.run_group_id,
                "is_latest": run.is_latest,
                "last_updated": run.last_updated,
            }
        except Exception as e:
            logger.error(f"Failed to get pipeline run {run_id}: {e}")
            raise

    def get_activity_runs(self, run_id: str) -> list:
        """
        Get all activity runs for a pipeline run.
        Returns list of activity run details including the failing activity.
        """
        try:
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(days=30)

            filter_params = RunFilterParameters(
                last_updated_after=start_time,
                last_updated_before=now,
            )

            result = self.client.activity_runs.query_by_pipeline_run(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                run_id=run_id,
                filter_parameters=filter_params,
            )

            activities = []
            for act in (result.value or []):
                activity = {
                    "activity_name": act.activity_name,
                    "activity_type": act.activity_type,
                    "status": act.status,
                    "start": act.activity_run_start,
                    "end": act.activity_run_end,
                    "duration_ms": act.duration_in_ms,
                    "error": None,
                    "input": act.input if hasattr(act, "input") else None,
                    "output": act.output if hasattr(act, "output") else None,
                }

                # Extract error details
                if act.error:
                    if isinstance(act.error, dict):
                        activity["error"] = {
                            "error_code": act.error.get("errorCode", ""),
                            "message": act.error.get("message", ""),
                            "failure_type": act.error.get("failureType", ""),
                            "target": act.error.get("target", ""),
                            "details": act.error.get("details", ""),
                        }
                    else:
                        activity["error"] = {"message": str(act.error)}

                activities.append(activity)

            logger.info(
                f"Found {len(activities)} activity run(s) for pipeline run {run_id}"
            )
            return activities
        except Exception as e:
            logger.error(f"Failed to get activity runs for {run_id}: {e}")
            raise

    def get_error_details(self, run_id: str) -> dict:
        """
        Extract comprehensive error details for a failed pipeline run.
        Combines pipeline-level and activity-level errors.
        """
        pipeline_details = self.get_pipeline_run_details(run_id)
        activity_runs = self.get_activity_runs(run_id)

        # Find failed activities
        failed_activities = [
            act for act in activity_runs if act["status"] == "Failed"
        ]

        # Build comprehensive error object
        error_details = {
            "pipeline_name": pipeline_details["pipeline_name"],
            "run_id": run_id,
            "status": pipeline_details["status"],
            "pipeline_message": pipeline_details.get("message", ""),
            "run_start": pipeline_details.get("run_start"),
            "run_end": pipeline_details.get("run_end"),
            "duration_ms": pipeline_details.get("duration_ms"),
            "parameters": pipeline_details.get("parameters", {}),
            "invoked_by": pipeline_details.get("invoked_by", {}),
            "total_activities": len(activity_runs),
            "failed_activities": failed_activities,
            "succeeded_activities": [
                act for act in activity_runs if act["status"] == "Succeeded"
            ],
            "all_activities": activity_runs,
        }

        # Extract the primary error
        if failed_activities:
            primary_error = failed_activities[0].get("error", {})
            error_details["primary_error_code"] = primary_error.get("error_code", "Unknown")
            error_details["primary_error_message"] = primary_error.get("message", "No error message available")
            error_details["primary_failure_type"] = primary_error.get("failure_type", "Unknown")
            error_details["failing_activity_name"] = failed_activities[0].get("activity_name", "Unknown")
            error_details["failing_activity_type"] = failed_activities[0].get("activity_type", "Unknown")
        else:
            error_details["primary_error_code"] = "Unknown"
            error_details["primary_error_message"] = pipeline_details.get("message", "No error message")
            error_details["primary_failure_type"] = "Unknown"
            error_details["failing_activity_name"] = "Unknown"
            error_details["failing_activity_type"] = "Unknown"

        return error_details

    def get_pipeline_history(
        self, pipeline_name: str, count: int = 10
    ) -> list:
        """
        Get the last N runs for a specific pipeline (any status).
        Useful for showing success/failure trends.
        """
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(days=30)

        filter_params = RunFilterParameters(
            last_updated_after=start_time,
            last_updated_before=now,
            filters=[
                RunQueryFilter(
                    operand=RunQueryFilterOperand.PIPELINE_NAME,
                    operator=RunQueryFilterOperator.EQUALS,
                    values=[pipeline_name],
                )
            ],
        )

        try:
            result = self.client.pipeline_runs.query_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
                filter_parameters=filter_params,
            )
            runs = list(result.value) if result.value else []
            # Sort by run start, most recent first
            runs.sort(
                key=lambda r: r.run_start if r.run_start else datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            return runs[:count]
        except Exception as e:
            logger.error(f"Failed to get pipeline history for {pipeline_name}: {e}")
            raise

    def list_pipelines(self) -> list:
        """List all pipelines in the data factory."""
        try:
            pipelines = self.client.pipelines.list_by_factory(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
            )
            return [p.name for p in pipelines]
        except Exception as e:
            logger.error(f"Failed to list pipelines: {e}")
            raise

    def test_connection(self) -> dict:
        """
        Test connectivity to Azure Data Factory.
        Returns connection status and factory details.
        """
        try:
            factory = self.client.factories.get(
                resource_group_name=self.resource_group,
                factory_name=self.factory_name,
            )
            pipelines = self.list_pipelines()
            return {
                "connected": True,
                "factory_name": factory.name,
                "location": factory.location,
                "provisioning_state": factory.provisioning_state,
                "pipeline_count": len(pipelines),
                "pipelines": pipelines,
            }
        except Exception as e:
            return {
                "connected": False,
                "error": str(e),
            }
