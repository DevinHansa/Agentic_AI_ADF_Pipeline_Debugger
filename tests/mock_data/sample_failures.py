"""
ADF Pipeline Debugger - Mock Data
Provides realistic sample pipeline failure data for testing and demo purposes.
"""
from datetime import datetime, timezone, timedelta


def get_mock_pipeline_failures():
    """Return a list of mock failed pipeline error details for demo/testing."""
    now = datetime.now(timezone.utc)

    return [
        {
            "pipeline_name": "ETL_Sales_Daily",
            "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "status": "Failed",
            "pipeline_message": "Activity 'Copy_Sales_Data' failed: The TCP/IP connection to the host 'sql-prod-server.database.windows.net', port 1433 has failed.",
            "run_start": now - timedelta(hours=2, minutes=15),
            "run_end": now - timedelta(hours=2, minutes=14, seconds=52),
            "duration_ms": 8000,
            "parameters": {
                "date": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
                "environment": "production",
                "batchSize": "10000",
            },
            "invoked_by": {"name": "DailyTrigger_0200", "type": "ScheduleTrigger"},
            "total_activities": 5,
            "failed_activities": [
                {
                    "activity_name": "Copy_Sales_Data",
                    "activity_type": "Copy",
                    "status": "Failed",
                    "start": now - timedelta(hours=2, minutes=15),
                    "end": now - timedelta(hours=2, minutes=14, seconds=52),
                    "duration_ms": 8000,
                    "error": {
                        "error_code": "2108",
                        "message": "The TCP/IP connection to the host 'sql-prod-server.database.windows.net', port 1433 has failed. Error: 'A network-related or instance-specific error occurred while establishing a connection to SQL Server. The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server is configured to allow remote connections.'",
                        "failure_type": "UserError",
                        "target": "Copy_Sales_Data",
                        "details": "",
                    },
                }
            ],
            "succeeded_activities": [
                {"activity_name": "Lookup_Config", "activity_type": "Lookup", "status": "Succeeded"},
                {"activity_name": "Set_Variables", "activity_type": "SetVariable", "status": "Succeeded"},
            ],
            "all_activities": [],
            "primary_error_code": "2108",
            "primary_error_message": "The TCP/IP connection to the host 'sql-prod-server.database.windows.net', port 1433 has failed. Error: 'A network-related or instance-specific error occurred while establishing a connection to SQL Server. The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server is configured to allow remote connections.'",
            "primary_failure_type": "UserError",
            "failing_activity_name": "Copy_Sales_Data",
            "failing_activity_type": "Copy",
        },
        {
            "pipeline_name": "Transform_Customer_360",
            "run_id": "b2c3d4e5-f6a7-8901-bcde-f23456789012",
            "status": "Failed",
            "pipeline_message": "Activity 'DataFlow_Customer_Merge' failed",
            "run_start": now - timedelta(hours=6, minutes=30),
            "run_end": now - timedelta(hours=5, minutes=10),
            "duration_ms": 4800000,
            "parameters": {
                "sourceSchema": "staging",
                "targetSchema": "gold",
                "runDate": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
            },
            "invoked_by": {"name": "NightlyBatch_2300", "type": "ScheduleTrigger"},
            "total_activities": 8,
            "failed_activities": [
                {
                    "activity_name": "DataFlow_Customer_Merge",
                    "activity_type": "ExecuteDataFlow",
                    "status": "Failed",
                    "start": now - timedelta(hours=6),
                    "end": now - timedelta(hours=5, minutes=10),
                    "duration_ms": 3000000,
                    "error": {
                        "error_code": "DF-EXEC-6",
                        "message": "Job failed due to reason: at Sink 'sinkCustomer': java.lang.OutOfMemoryError: Java heap space. The cluster ran out of memory during data flow execution. Consider increasing the cluster size or reducing the data volume.",
                        "failure_type": "SystemError",
                        "target": "DataFlow_Customer_Merge",
                        "details": "",
                    },
                }
            ],
            "succeeded_activities": [
                {"activity_name": "Lookup_Sources", "activity_type": "Lookup", "status": "Succeeded"},
                {"activity_name": "Copy_Staging_Data", "activity_type": "Copy", "status": "Succeeded"},
                {"activity_name": "Validate_Schema", "activity_type": "Lookup", "status": "Succeeded"},
                {"activity_name": "Set_RunDate", "activity_type": "SetVariable", "status": "Succeeded"},
            ],
            "all_activities": [],
            "primary_error_code": "DF-EXEC-6",
            "primary_error_message": "Job failed due to reason: at Sink 'sinkCustomer': java.lang.OutOfMemoryError: Java heap space. The cluster ran out of memory during data flow execution. Consider increasing the cluster size or reducing the data volume.",
            "primary_failure_type": "SystemError",
            "failing_activity_name": "DataFlow_Customer_Merge",
            "failing_activity_type": "ExecuteDataFlow",
        },
        {
            "pipeline_name": "Ingest_API_Events",
            "run_id": "c3d4e5f6-a7b8-9012-cdef-345678901234",
            "status": "Failed",
            "pipeline_message": "Activity 'Copy_API_Events' failed: The file 'events/2026/02/25/events_20260225.json' was not found",
            "run_start": now - timedelta(hours=1, minutes=30),
            "run_end": now - timedelta(hours=1, minutes=29, seconds=45),
            "duration_ms": 15000,
            "parameters": {
                "date": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
                "apiEndpoint": "events",
                "container": "raw-data",
            },
            "invoked_by": {"name": "HourlyTrigger", "type": "ScheduleTrigger"},
            "total_activities": 3,
            "failed_activities": [
                {
                    "activity_name": "Copy_API_Events",
                    "activity_type": "Copy",
                    "status": "Failed",
                    "start": now - timedelta(hours=1, minutes=30),
                    "end": now - timedelta(hours=1, minutes=29, seconds=45),
                    "duration_ms": 15000,
                    "error": {
                        "error_code": "BlobNotFound",
                        "message": "The specified blob 'events/2026/02/25/events_20260225.json' does not exist. ErrorCode: BlobNotFound. Container: raw-data, Account: prodstorageaccount.",
                        "failure_type": "UserError",
                        "target": "Copy_API_Events",
                        "details": "",
                    },
                }
            ],
            "succeeded_activities": [
                {"activity_name": "Get_Date_Parameter", "activity_type": "SetVariable", "status": "Succeeded"},
            ],
            "all_activities": [],
            "primary_error_code": "BlobNotFound",
            "primary_error_message": "The specified blob 'events/2026/02/25/events_20260225.json' does not exist. ErrorCode: BlobNotFound. Container: raw-data, Account: prodstorageaccount.",
            "primary_failure_type": "UserError",
            "failing_activity_name": "Copy_API_Events",
            "failing_activity_type": "Copy",
        },
    ]
