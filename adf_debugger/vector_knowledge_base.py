"""
ADF Pipeline Debugger - Vector Knowledge Base
Semantic vector-based knowledge base using ChromaDB + sentence-transformers.
Provides intelligent error matching beyond simple regex patterns.
"""
import json
import logging
import os
from pathlib import Path
from typing import List, Optional

import chromadb
from chromadb.config import Settings

logger = logging.getLogger("adf_debugger.vector_kb")

# Comprehensive ADF error knowledge entries scraped from Azure docs and community
ADF_ERROR_KNOWLEDGE = [
    # ── Connectivity Errors ──
    {
        "id": "conn_tcp_sql",
        "category": "connectivity",
        "severity": "high",
        "title": "SQL Server TCP/IP Connection Failure",
        "error_pattern": "TCP/IP connection to the host failed",
        "description": "The pipeline cannot establish a TCP/IP connection to SQL Server. This typically happens when the server is unreachable, the port is blocked, or the SQL Server instance is not running.",
        "causes": [
            "SQL Server is not running or not listening on the expected port",
            "Firewall rules blocking port 1433 (or custom port)",
            "Network Security Group (NSG) rules blocking traffic",
            "VNet/subnet misconfiguration preventing connectivity",
            "Self-hosted Integration Runtime cannot reach the database",
            "DNS resolution failure for the server hostname"
        ],
        "solutions": [
            "Verify SQL Server is running: Check Azure Portal > SQL Server > Status",
            "Check firewall rules: Azure Portal > SQL Server > Networking > Allow Azure services",
            "Test connectivity: Run Test-NetConnection from SHIR machine to server:port",
            "Verify NSG rules allow inbound traffic on the SQL port",
            "Check if private endpoint is configured correctly",
            "Verify DNS resolution: nslookup <server>.database.windows.net"
        ],
        "prevention": [
            "Set up connection monitoring alerts",
            "Use managed private endpoints for secure connectivity",
            "Implement retry policies with exponential backoff",
            "Add a Lookup activity as a connectivity check before data operations"
        ],
        "estimated_fix_time": "15-45 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-troubleshoot-guide"]
    },
    {
        "id": "conn_timeout",
        "category": "connectivity",
        "severity": "high",
        "title": "Connection Timeout Error",
        "error_pattern": "connection timed out|The operation has timed out|Connection timeout expired",
        "description": "A data source connection attempt timed out before completing. This indicates the target system is either overloaded, unreachable, or too slow to respond.",
        "causes": [
            "Target server is overloaded or unresponsive",
            "Network latency between ADF and the data source",
            "Firewall or security appliance blocking/slowing connections",
            "Self-hosted IR machine has limited network bandwidth",
            "Connection pool exhaustion on the data source",
            "DNS resolution delays"
        ],
        "solutions": [
            "Increase timeout settings in the linked service configuration",
            "Check target server health and performance metrics",
            "Move to a closer Azure region to reduce latency",
            "Use Azure IR instead of Self-hosted IR if possible",
            "Verify network path with traceroute/pathping",
            "Check integration runtime resource usage (CPU/memory)"
        ],
        "prevention": [
            "Set appropriate timeout values (default may be too low)",
            "Implement circuit breaker pattern with retry policies",
            "Monitor server health proactively",
            "Schedule resource-intensive pipelines during off-peak hours"
        ],
        "estimated_fix_time": "15-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-troubleshoot-guide"]
    },
    {
        "id": "conn_ssl_tls",
        "category": "connectivity",
        "severity": "high",
        "title": "SSL/TLS Connection Error",
        "error_pattern": "SSL|TLS|certificate|The underlying connection was closed|trust relationship",
        "description": "SSL/TLS handshake or certificate validation failed during connection attempt.",
        "causes": [
            "Self-signed or expired SSL certificate on the data source",
            "TLS version mismatch between ADF and target server",
            "Certificate chain is incomplete or untrusted",
            "Network appliance intercepting/modifying SSL traffic"
        ],
        "solutions": [
            "Update the SSL certificate on the target server",
            "Set encrypt=false in the connection string for testing (not recommended for production)",
            "Import the CA certificate into the SHIR machine's trusted root store",
            "Check TLS version compatibility: try TLS 1.2",
            "Verify certificate chain using openssl s_client -connect host:port"
        ],
        "prevention": [
            "Set up certificate expiration monitoring",
            "Use Azure-managed certificates where possible",
            "Document SSL configuration for all linked services"
        ],
        "estimated_fix_time": "20-60 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-troubleshoot-guide"]
    },
    # ── Authentication Errors ──
    {
        "id": "auth_login_failed",
        "category": "authentication",
        "severity": "critical",
        "title": "Authentication/Login Failure",
        "error_pattern": "Login failed|Authentication failed|password authentication failed|Invalid credentials|Access denied",
        "description": "The credentials provided in the linked service are incorrect, expired, or the user account is locked/disabled.",
        "causes": [
            "Username or password is incorrect in the linked service",
            "Password has expired or was recently rotated",
            "Account is locked due to too many failed attempts",
            "Service principal secret/certificate has expired",
            "Managed identity is not properly configured",
            "IP not in allowed list for the target resource"
        ],
        "solutions": [
            "Verify credentials in the linked service configuration",
            "Test connection using the 'Test Connection' button in ADF",
            "Check if the password was recently changed and update linked service",
            "For service principals: renew the secret in Azure AD",
            "For managed identity: verify the identity is assigned to the ADF instance",
            "Check Azure AD sign-in logs for detailed error information"
        ],
        "prevention": [
            "Use Azure Key Vault for credential management with auto-rotation",
            "Set up alerts for credential expiration",
            "Use managed identity authentication where possible",
            "Implement regular credential rotation procedures"
        ],
        "estimated_fix_time": "10-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/store-credentials-in-key-vault"]
    },
    {
        "id": "auth_aad_token",
        "category": "authentication",
        "severity": "critical",
        "title": "Azure AD Token Acquisition Failure",
        "error_pattern": "AADSTS|Failed to acquire token|token|service principal|InvalidClientSecret|unauthorized_client",
        "description": "Azure Active Directory could not issue an authentication token, preventing access to Azure resources.",
        "causes": [
            "Service principal client secret has expired",
            "Service principal has been deleted from Azure AD",
            "Incorrect tenant ID configured",
            "Application permission consent not granted",
            "Managed identity not enabled on the ADF instance",
            "Azure AD service is experiencing issues"
        ],
        "solutions": [
            "Regenerate service principal secret: Azure Portal > App Registrations > Certificates & Secrets",
            "Verify tenant ID matches the Azure AD tenant",
            "Grant admin consent for required API permissions",
            "Enable system-assigned or user-assigned managed identity on ADF",
            "Check Azure AD service health at status.azure.com"
        ],
        "prevention": [
            "Monitor service principal secret expiration dates",
            "Use managed identity instead of service principals where possible",
            "Set up Key Vault with auto-rotation for secrets",
            "Create alerts for Azure AD authentication failures"
        ],
        "estimated_fix_time": "15-45 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-data-lake-storage"]
    },
    # ── Permission Errors ──
    {
        "id": "perm_forbidden",
        "category": "permission",
        "severity": "high",
        "title": "Insufficient Permissions / Forbidden Access",
        "error_pattern": "Forbidden|403|insufficient privileges|does not have permission|AuthorizationFailed|AccessDenied",
        "description": "The authenticated identity lacks the necessary RBAC roles or ACL permissions to perform the requested operation.",
        "causes": [
            "Missing RBAC role assignment (e.g., Storage Blob Data Contributor)",
            "Firewall/VNET rules blocking the request",
            "ACL permissions not set on specific files/directories in ADLS Gen2",
            "Managed identity not granted access to the target resource",
            "Conditional Access policies blocking the request"
        ],
        "solutions": [
            "Assign the correct RBAC role: e.g., Storage Blob Data Contributor for ADLS Gen2",
            "Add ADF's managed identity to the target resource's access control",
            "Check ADLS Gen2 ACL permissions on the specific path",
            "Verify firewall rules allow ADF's outbound IPs or VNet",
            "Use 'Allow trusted Microsoft services' option in storage firewall"
        ],
        "prevention": [
            "Document all required permissions in a runbook",
            "Use managed identity with principle of least privilege",
            "Set up alerts for authorization failures in Azure Monitor",
            "Regularly audit RBAC assignments"
        ],
        "estimated_fix_time": "10-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-data-lake-storage"]
    },
    # ── Data Quality Errors ──
    {
        "id": "dq_file_not_found",
        "category": "missing_data",
        "severity": "medium",
        "title": "File or Path Not Found",
        "error_pattern": "FileNotFound|PathNotFound|NotFound|BlobNotFound|The specified path does not exist|The specified blob does not exist|ResourceNotFound",
        "description": "The source file, blob, or path specified in the dataset does not exist at the expected location.",
        "causes": [
            "File has not been delivered by upstream system yet",
            "File path contains incorrect date/time parameters",
            "Dynamic path expression evaluates to a non-existent path",
            "File was moved, renamed, or deleted",
            "Case-sensitive file system and path case doesn't match",
            "Container or file system doesn't exist in the storage account"
        ],
        "solutions": [
            "Verify the file exists: Azure Portal > Storage Account > Containers > Browse",
            "Check dynamic path expressions: use Debug mode to see resolved values",
            "Add a GetMetadata activity to check file existence before Copy",
            "Add a Validation activity to wait for file arrival with timeout",
            "Verify the container/file system name is exactly correct (case-sensitive)",
            "Check upstream pipeline or process completed successfully"
        ],
        "prevention": [
            "Always use GetMetadata or Validation activities before file operations",
            "Implement file arrival SLAs with monitoring",
            "Add retry logic with Wait activities for delayed files",
            "Use event triggers based on file creation events",
            "Log expected vs actual file paths for debugging"
        ],
        "estimated_fix_time": "10-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/control-flow-validation-activity"]
    },
    {
        "id": "dq_schema_mismatch",
        "category": "schema",
        "severity": "high",
        "title": "Schema/Column Mismatch Error",
        "error_pattern": "schema|column.*not found|mapping.*invalid|ColumnNotFound|InvalidColumn|The column .* cannot be found|type mismatch|data type",
        "description": "The source data schema doesn't match the expected schema defined in the dataset or mapping, causing copy or data flow failures.",
        "causes": [
            "Source data has new/renamed/removed columns",
            "Column mapping references non-existent columns",
            "Data type incompatibility between source and sink",
            "Schema drift in the source system",
            "Header row missing or incorrectly detected in CSV files",
            "Encoding issues causing column names to be garbled"
        ],
        "solutions": [
            "Compare source schema with dataset definition",
            "Enable schema drift in data flows to handle dynamic schemas",
            "Update the column mapping in the copy activity",
            "Use 'Import Schema' button to refresh the dataset schema",
            "Add explicit data type conversions in the mapping",
            "Check CSV delimiter settings and header row configuration"
        ],
        "prevention": [
            "Enable schema drift tolerance in data flows",
            "Implement schema validation as a pre-check activity",
            "Document expected schemas and set up change detection alerts",
            "Use schema-on-read formats (e.g., Parquet with schema evolution)"
        ],
        "estimated_fix_time": "15-60 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping"]
    },
    {
        "id": "dq_data_truncation",
        "category": "data_quality",
        "severity": "medium",
        "title": "Data Truncation Error",
        "error_pattern": "truncat|String or binary data would be truncated|data too long|value too large|overflow",
        "description": "Data from the source exceeds the maximum size of the destination column, causing a truncation error.",
        "causes": [
            "Source data length exceeds destination column size",
            "Numeric value exceeds destination column precision",
            "Unicode data being stored in non-Unicode column",
            "Date/time format incompatibility"
        ],
        "solutions": [
            "Increase the destination column size (ALTER TABLE)",
            "Add a data flow transformation to truncate/validate data before loading",
            "Use CAST/CONVERT in a pre-copy stored procedure",
            "Enable fault tolerance to skip incompatible rows",
            "Identify the offending rows: SELECT MAX(LEN(column)) FROM source"
        ],
        "prevention": [
            "Design destination tables with adequate column sizes",
            "Add data validation transformations in data flows",
            "Enable copy activity fault tolerance with logging"
        ],
        "estimated_fix_time": "15-45 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-fault-tolerance"]
    },
    {
        "id": "dq_encoding",
        "category": "data_quality",
        "severity": "medium",
        "title": "Character Encoding / Invalid Data Format",
        "error_pattern": "encoding|invalid character|malformed|corrupt|codec|UTF-8|parse error|InvalidDataField",
        "description": "The source data contains characters or formatting that cannot be properly decoded or parsed.",
        "causes": [
            "File encoding doesn't match the configured encoding (e.g., UTF-8 vs Latin-1)",
            "BOM (Byte Order Mark) present or missing",
            "Corrupted file from upload or transfer process",
            "Invalid JSON/XML structure",
            "Mixed line endings (CRLF/LF) causing parsing issues"
        ],
        "solutions": [
            "Specify the correct encoding in the dataset properties",
            "Use a hex editor or notepad++ to check the actual file encoding",
            "Validate file integrity (compare checksums with source)",
            "For JSON: validate structure using a JSON validator tool",
            "For CSV: check delimiter consistency across all rows"
        ],
        "prevention": [
            "Standardize file encoding to UTF-8 across all systems",
            "Add file validation activities at pipeline start",
            "Use schema-on-read formats (Parquet, Avro) to avoid encoding issues"
        ],
        "estimated_fix_time": "15-45 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/supported-file-formats-and-compression-codecs"]
    },
    # ── Resource / Quota Errors ──
    {
        "id": "res_out_of_memory",
        "category": "resource",
        "severity": "critical",
        "title": "Out of Memory Error (OOM)",
        "error_pattern": "OutOfMemory|out of memory|DF-Executor-OutOfMemoryError|heap space|GC overhead|MemoryError",
        "description": "The data flow or copy activity ran out of memory, causing it to fail. This typically occurs with large datasets or complex transformations.",
        "causes": [
            "Data flow processing dataset larger than allocated memory",
            "Too many columns or complex transformations",
            "Broadcast join with a large dataset",
            "Insufficient cluster size for the data volume",
            "Memory leak in custom code or expressions",
            "Skewed data causing one partition to be much larger than others"
        ],
        "solutions": [
            "Increase the data flow cluster size (Core count)",
            "Use hash join instead of broadcast join for large tables",
            "Add partition transformations to distribute data evenly",
            "Reduce the number of columns early in the pipeline (select/project)",
            "Split large data flows into smaller, sequential steps",
            "Enable staging for copy activity with large datasets"
        ],
        "prevention": [
            "Right-size clusters based on data volume",
            "Monitor memory usage in data flow debug mode",
            "Use optimized partition strategies for large datasets",
            "Implement data sampling for development/testing"
        ],
        "estimated_fix_time": "30-90 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/data-flow-troubleshoot-guide"]
    },
    {
        "id": "res_quota_exceeded",
        "category": "quota",
        "severity": "high",
        "title": "Quota / Rate Limit Exceeded",
        "error_pattern": "quota|rate limit|429|TooManyRequests|throttl|concurrent|limit exceeded|exceeded.*limit",
        "description": "The operation exceeded Azure resource quotas or API rate limits.",
        "causes": [
            "Too many concurrent pipeline runs",
            "Too many concurrent copy activities",
            "API rate limiting from the target service",
            "Data Integration Units (DIU) quota exceeded",
            "Azure subscription-level quotas hit"
        ],
        "solutions": [
            "Reduce concurrent pipeline/activity execution",
            "Implement exponential backoff retry policies",
            "Request quota increase: Azure Portal > Subscriptions > Usage + quotas",
            "Reduce DIU count on copy activities",
            "Stagger pipeline schedules to avoid burst traffic",
            "Use batch operations instead of individual API calls"
        ],
        "prevention": [
            "Monitor quota usage with Azure Monitor alerts",
            "Design pipelines with throttling in mind",
            "Use queue-based load leveling patterns",
            "Set up capacity planning reviews"
        ],
        "estimated_fix_time": "15-60 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/data-factory-service-limits"]
    },
    {
        "id": "res_disk_space",
        "category": "resource",
        "severity": "high",
        "title": "Disk Space / Storage Exhausted",
        "error_pattern": "disk space|OutOfDiskSpaceError|No space left|storage full|BlockCountExceedsLimitError",
        "description": "The operation ran out of disk space on the integration runtime or exceeded blob storage block limits.",
        "causes": [
            "SHIR machine disk is full (temp files, logs)",
            "Blob storage block count limit (50,000 blocks) exceeded",
            "Staging area is full",
            "Large temp files from sorting/aggregation operations"
        ],
        "solutions": [
            "Clean up disk space on SHIR machine",
            "Increase staging storage capacity",
            "Reduce file size by splitting into smaller chunks",
            "Use append blob or page blob for very large files",
            "Clear old pipeline run logs and temp files"
        ],
        "prevention": [
            "Monitor disk space on SHIR machines",
            "Set up auto-cleanup for temp and log files",
            "Implement data archival strategies"
        ],
        "estimated_fix_time": "15-45 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/data-flow-troubleshoot-guide"]
    },
    # ── Timeout Errors ──
    {
        "id": "timeout_activity",
        "category": "timeout",
        "severity": "medium",
        "title": "Activity Timeout",
        "error_pattern": "timeout|TimeoutException|activity timed out|execution expired|exceeded.*timeout",
        "description": "An activity exceeded its configured timeout limit before completing.",
        "causes": [
            "Long-running query on the data source",
            "Large data volume taking longer than expected",
            "Resource contention on the target system",
            "Network congestion causing slow transfers",
            "Default timeout too short for the operation"
        ],
        "solutions": [
            "Increase the activity timeout in pipeline settings",
            "Optimize the source query (add indexes, reduce scope)",
            "Increase Data Integration Units (DIU) for copy activities",
            "Split the operation into smaller batches with parallelism",
            "Check target system performance metrics"
        ],
        "prevention": [
            "Set appropriate timeout values based on historical run times",
            "Monitor activity duration trends",
            "Implement incremental loading patterns",
            "Optimize queries before deploying to production"
        ],
        "estimated_fix_time": "15-60 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/concepts-pipeline-execution-triggers"]
    },
    # ── Configuration Errors ──
    {
        "id": "config_linked_service",
        "category": "configuration",
        "severity": "medium",
        "title": "Linked Service Configuration Error",
        "error_pattern": "linked service|connection string|InvalidConnectionString|IncorrectLinkedServiceConfiguration|connection.*invalid",
        "description": "The linked service configuration is invalid, preventing connection to the data source.",
        "causes": [
            "Connection string format is incorrect",
            "Missing required connection properties",
            "Key Vault reference is invalid or inaccessible",
            "Integration runtime not assigned or unavailable",
            "Parameterized values not resolving correctly"
        ],
        "solutions": [
            "Verify connection string format matches the connector documentation",
            "Test the connection using 'Test Connection' in ADF",
            "Check Key Vault access policies for ADF managed identity",
            "Verify the integration runtime is online and healthy",
            "Check parameter values in debug mode"
        ],
        "prevention": [
            "Always test connections after configuration changes",
            "Use parameterized linked services with default values",
            "Monitor integration runtime health"
        ],
        "estimated_fix_time": "10-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/concepts-linked-services"]
    },
    {
        "id": "config_ir_offline",
        "category": "configuration",
        "severity": "critical",
        "title": "Integration Runtime Offline / Unavailable",
        "error_pattern": "integration runtime|IR.*offline|IR.*unavailable|self-hosted.*not running|SHIROFFLINE",
        "description": "The Self-hosted Integration Runtime is offline or cannot be reached by ADF.",
        "causes": [
            "SHIR service stopped on the host machine",
            "Host machine was restarted or is offline",
            "Network connectivity issue between ADF and SHIR",
            "SHIR version is outdated and incompatible",
            "Windows service crashed due to resource exhaustion"
        ],
        "solutions": [
            "Check SHIR status: Open SHIR config manager on the host machine",
            "Restart the 'Microsoft Integration Runtime' Windows service",
            "Verify the machine is running and accessible",
            "Update SHIR to the latest version",
            "Check SHIR diagnostic logs in Event Viewer"
        ],
        "prevention": [
            "Set up SHIR monitoring and auto-restart",
            "Use high-availability SHIR with multiple nodes",
            "Monitor SHIR health from Azure Monitor",
            "Keep SHIR updated to the latest version"
        ],
        "estimated_fix_time": "10-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/create-self-hosted-integration-runtime"]
    },
    {
        "id": "config_expression",
        "category": "configuration",
        "severity": "medium",
        "title": "Expression / Parameter Evaluation Error",
        "error_pattern": "expression|parameter|InvalidTemplate|ParameterParseError|dynamic content|@{|concat|pipeline().parameters",
        "description": "A pipeline expression or parameter reference failed to evaluate correctly.",
        "causes": [
            "Syntax error in dynamic content expression",
            "Referencing a parameter that doesn't exist",
            "Type mismatch in expression operations",
            "Missing quotes around string values in expressions",
            "Nested expression depth too deep"
        ],
        "solutions": [
            "Validate expressions using the expression builder with sample data",
            "Check that all referenced parameters are defined with correct types",
            "Use @string() to ensure string type in concatenations",
            "Test expressions step-by-step in a debug session",
            "Simplify complex expressions by breaking into Set Variable activities"
        ],
        "prevention": [
            "Test all expressions in debug mode before publishing",
            "Use consistent naming conventions for parameters",
            "Document expected parameter types and values"
        ],
        "estimated_fix_time": "10-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/control-flow-expression-language-functions"]
    },
    # ── Data Flow Specific Errors ──
    {
        "id": "df_broadcast_failure",
        "category": "resource",
        "severity": "high",
        "title": "Data Flow Broadcast Timeout / Failure",
        "error_pattern": "BroadcastTimeout|BroadcastFailure|broadcast.*timeout|DF-Executor-BroadcastTimeout",
        "description": "A broadcast join in the data flow timed out because the broadcast dataset was too large to distribute to all worker nodes.",
        "causes": [
            "Dataset being broadcast is too large (>8GB compressed)",
            "Insufficient cluster resources for the broadcast",
            "Network issues between Spark nodes",
            "Complex transformations before the join"
        ],
        "solutions": [
            "Switch from Broadcast join to Hash/Sort Merge join",
            "Increase data flow cluster size",
            "Filter or aggregate the smaller dataset before joining",
            "Set broadcast timeout to 'None' and let Spark decide",
            "Increase broadcast threshold in optimization settings"
        ],
        "prevention": [
            "Profile data sizes before choosing join strategy",
            "Use 'Auto' join optimization to let Spark choose",
            "Monitor data volume trends"
        ],
        "estimated_fix_time": "20-60 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/data-flow-troubleshoot-guide"]
    },
    {
        "id": "df_spark_error",
        "category": "resource",
        "severity": "high",
        "title": "Spark Cluster / Internal Server Error",
        "error_pattern": "InternalServerError|Spark|cluster.*failed|DF-Executor-InternalServerError|SystemErrorSynapseSparkJobFailed|An internal error occurred",
        "description": "The data flow's underlying Spark cluster encountered an internal error.",
        "causes": [
            "Transient Spark infrastructure issue",
            "Cluster startup failure due to capacity constraints",
            "Incompatible library versions",
            "Azure region capacity limitations"
        ],
        "solutions": [
            "Retry the pipeline run (transient failures often resolve on retry)",
            "Change the data flow compute type or cluster size",
            "Check Azure service health for the region",
            "Use TTL (time-to-live) to keep warm clusters"
        ],
        "prevention": [
            "Set retry policies on data flow activities",
            "Use reserved capacity for critical pipelines",
            "Monitor Azure service health dashboard"
        ],
        "estimated_fix_time": "5-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/data-flow-troubleshoot-guide"]
    },
    # ── Copy Activity Specific Errors ──
    {
        "id": "copy_user_error",
        "category": "configuration",
        "severity": "medium",
        "title": "Copy Activity User Error",
        "error_pattern": "UserErrorFileNotFound|UserError|ErrorCode=User|Type=Microsoft.DataTransfer",
        "description": "The copy activity failed due to a user-configurable issue rather than a system error.",
        "causes": [
            "Incorrect file path or connection settings",
            "Source data format doesn't match dataset configuration",
            "Missing required parameters or properties",
            "Permissions issue on source or sink",
            "Incompatible data types between source and sink"
        ],
        "solutions": [
            "Review the full error message for specific ErrorCode",
            "Verify dataset configuration (file path, format, delimiter)",
            "Test the linked service connection",
            "Check permissions on both source and sink",
            "Validate data using a preview in ADF"
        ],
        "prevention": [
            "Use GetMetadata to validate source before copy",
            "Add data preview validation in debug runs",
            "Document all dataset configurations"
        ],
        "estimated_fix_time": "15-45 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-overview"]
    },
    {
        "id": "copy_jre_not_found",
        "category": "configuration",
        "severity": "high",
        "title": "Java Runtime Not Found on SHIR",
        "error_pattern": "JreNotFound|Java Runtime Environment cannot be found|20000",
        "description": "The Self-hosted Integration Runtime cannot find Java Runtime, required for Parquet/ORC file operations.",
        "causes": [
            "Java Runtime not installed on SHIR machine",
            "JAVA_HOME environment variable not set correctly",
            "Wrong Java version installed (32-bit vs 64-bit)"
        ],
        "solutions": [
            "Install Java Runtime (JRE 8 or later) on the SHIR machine",
            "Set JAVA_HOME environment variable to the JRE installation path",
            "Ensure 64-bit JRE is installed for 64-bit SHIR",
            "Restart the Integration Runtime service after installing Java"
        ],
        "prevention": [
            "Include Java installation in SHIR setup checklist",
            "Monitor SHIR prerequisites in regular health checks"
        ],
        "estimated_fix_time": "15-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/format-parquet"]
    },
    # ── ADLS Gen2 Specific Errors ──
    {
        "id": "adls_invalid_status",
        "category": "connectivity",
        "severity": "high",
        "title": "ADLS Gen2 Operation Returned Invalid Status Code",
        "error_pattern": "ADLS Gen2 operation failed|invalid status code|StorageAccountNotFound|ContainerNotFound|FilesystemNotFound",
        "description": "An operation against Azure Data Lake Storage Gen2 failed with an unexpected HTTP status code.",
        "causes": [
            "Storage account doesn't exist or is in a different subscription",
            "Container/filesystem doesn't exist",
            "Firewall rules blocking ADF access",
            "Account key or SAS token is invalid/expired",
            "Soft delete or versioning causing unexpected behavior"
        ],
        "solutions": [
            "Verify storage account exists and is accessible",
            "Create the container/filesystem if it doesn't exist",
            "Add ADF's managed identity or IPs to storage firewall exceptions",
            "Regenerate and update the account key or SAS token",
            "Check the full HTTP status code for specific error details"
        ],
        "prevention": [
            "Use managed identity authentication for ADLS",
            "Set up storage account health monitoring",
            "Use terraform/ARM to ensure infrastructure consistency"
        ],
        "estimated_fix_time": "15-45 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-data-lake-storage"]
    },
    # ── SQL Specific Errors ──
    {
        "id": "sql_deadlock",
        "category": "data_quality",
        "severity": "high",
        "title": "SQL Deadlock / Lock Timeout",
        "error_pattern": "deadlock|lock request time out|blocked|1205|Lock escalation",
        "description": "SQL operations are blocked by concurrent locks, causing deadlocks or lock timeouts.",
        "causes": [
            "Concurrent writes to the same table/rows",
            "Long-running transactions holding locks",
            "Lock escalation from row to table level",
            "Missing indexes causing table scans"
        ],
        "solutions": [
            "Implement retry logic for deadlock errors (error 1205)",
            "Optimize queries to minimize lock duration",
            "Add appropriate indexes to reduce table scans",
            "Use NOLOCK or READ UNCOMMITTED for read operations where appropriate",
            "Schedule conflicting operations at different times"
        ],
        "prevention": [
            "Design table structures to minimize contention",
            "Use batch inserts instead of row-by-row operations",
            "Monitor lock wait statistics"
        ],
        "estimated_fix_time": "20-60 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-sql-database"]
    },
    {
        "id": "sql_firewall",
        "category": "connectivity",
        "severity": "high",
        "title": "SQL Server Firewall Rule Blocking Access",
        "error_pattern": "firewall|40615|is not allowed to access the server|denied by the server firewall|Client IP address is not authorized",
        "description": "The SQL Server firewall is blocking connections from the ADF integration runtime's IP address.",
        "causes": [
            "ADF's IP addresses not in SQL Server firewall rules",
            "Azure services access not enabled",
            "Private endpoint not configured for private connectivity",
            "IP address changed due to Azure IR scaling"
        ],
        "solutions": [
            "Add ADF's IP range to SQL firewall: Portal > SQL Server > Networking",
            "Enable 'Allow Azure services and resources to access this server'",
            "Use managed private endpoints for secure connectivity",
            "Use managed VNet integration runtime"
        ],
        "prevention": [
            "Use managed private endpoints for all production connections",
            "Monitor firewall rule changes with Azure Policy",
            "Document all required firewall configurations"
        ],
        "estimated_fix_time": "5-15 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/azure-sql/database/firewall-configure"]
    },
    # ── Pipeline Execution Errors ──
    {
        "id": "pipe_circular_dependency",
        "category": "configuration",
        "severity": "medium",
        "title": "Pipeline Circular Dependency",
        "error_pattern": "circular|dependency|cycle detected|recursive",
        "description": "The pipeline contains circular activity dependencies, preventing execution.",
        "causes": [
            "Activity A depends on Activity B which depends on Activity A",
            "Complex conditional branches creating loops",
            "Incorrect 'Depends On' configuration"
        ],
        "solutions": [
            "Review the pipeline dependency graph in the visual editor",
            "Remove or restructure circular dependencies",
            "Use ForEach or Until loops instead of circular dependencies"
        ],
        "prevention": [
            "Plan pipeline flow on paper/whiteboard before building",
            "Use naming conventions that reflect execution order"
        ],
        "estimated_fix_time": "15-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/concepts-pipelines-activities"]
    },
    {
        "id": "pipe_trigger_failure",
        "category": "configuration",
        "severity": "medium",
        "title": "Trigger Failure / Missed Trigger",
        "error_pattern": "trigger|scheduled|tumbling window|event trigger|TriggerFailed",
        "description": "A pipeline trigger failed to fire or execute the pipeline.",
        "causes": [
            "Trigger is stopped/not started",
            "Schedule cron expression is incorrect",
            "Event-based trigger misconfigured (path/pattern)",
            "Previous tumbling window still running",
            "Dependency chain in tumbling window not met"
        ],
        "solutions": [
            "Verify trigger status: ADF > Manage > Triggers",
            "Check trigger run history for specific errors",
            "Validate schedule expression against expected times",
            "For event triggers: verify the event subscription in Azure Portal",
            "For tumbling windows: check previous window run status"
        ],
        "prevention": [
            "Monitor trigger health with Azure Monitor alerts",
            "Set up dead-letter notifications for missed triggers",
            "Test triggers thoroughly in debug before publishing"
        ],
        "estimated_fix_time": "10-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/concepts-pipeline-execution-triggers"]
    },
    # ── Network / ODBC Errors ──
    {
        "id": "net_odbc_invalid",
        "category": "configuration",
        "severity": "medium",
        "title": "ODBC Invalid Query / Connection Error",
        "error_pattern": "ODBC|9611|UserErrorOdbcInvalidQueryString|OdbcOperationFailed|driver|DSN",
        "description": "An ODBC-based connection or query failed, typically with non-Microsoft data sources.",
        "causes": [
            "Invalid SQL query syntax for the target data source",
            "ODBC driver not installed on SHIR machine",
            "DSN not configured properly",
            "Query contains unsupported functions for the target system"
        ],
        "solutions": [
            "Validate the query directly against the data source",
            "Install the correct ODBC driver on the SHIR machine",
            "Verify DSN configuration using Windows ODBC Data Sources",
            "Use Script activity for non-query scripts"
        ],
        "prevention": [
            "Test queries directly before using in pipeline",
            "Document required ODBC drivers for each data source",
            "Use parameterized queries to avoid injection issues"
        ],
        "estimated_fix_time": "15-45 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-odbc"]
    },
    # ── Cosmos DB Specific ──
    {
        "id": "cosmos_key_invalid",
        "category": "authentication",
        "severity": "high",
        "title": "Cosmos DB Invalid Account Key / Configuration",
        "error_pattern": "Cosmos|CosmosDb|InvalidAccountKey|InvalidAccountConfiguration|DF-Cosmos",
        "description": "The Cosmos DB connection failed due to invalid account key or configuration.",
        "causes": [
            "Account key was rotated and not updated in linked service",
            "Wrong database or container name",
            "Incorrect connection mode (Gateway vs Direct)",
            "Cosmos DB account is in a different region or disabled"
        ],
        "solutions": [
            "Update the account key from Azure Portal > Cosmos DB > Keys",
            "Verify database and container names are exactly correct",
            "Try switching connection mode between Gateway and Direct",
            "Check Cosmos DB account status in Azure Portal"
        ],
        "prevention": [
            "Store Cosmos DB keys in Azure Key Vault with rotation alerts",
            "Use managed identity authentication where supported"
        ],
        "estimated_fix_time": "10-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-cosmos-db"]
    },
    # ── Mapping Data Flow Specific ──
    {
        "id": "df_partition_error",
        "category": "configuration",
        "severity": "medium",
        "title": "Data Flow Partition / File Error",
        "error_pattern": "PartitionDirectoryError|InvalidPartitionFileNames|partition|InvalidSparkFolder",
        "description": "Data flow failed due to invalid partitioning configuration or file names in the output.",
        "causes": [
            "Invalid characters in partition column values",
            "Partition directories already exist with incompatible format",
            "Spark folder structure doesn't match expected format"
        ],
        "solutions": [
            "Clean partition column values (remove special characters)",
            "Delete existing partition directories before rewrite",
            "Specify explicit partition pattern in sink settings",
            "Use 'Clear the folder' option in sink settings"
        ],
        "prevention": [
            "Validate partition column values before writing",
            "Use deterministic partition strategies"
        ],
        "estimated_fix_time": "15-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/data-flow-troubleshoot-guide"]
    },
    # ── PostgreSQL / MySQL Specific ──
    {
        "id": "pg_connection_failed",
        "category": "connectivity",
        "severity": "high",
        "title": "PostgreSQL / MySQL Connection Failure",
        "error_pattern": "PostgreSQL|MySQL|pg_hba|28P01|28000|password authentication failed for user|Failed to connect.*flexible server",
        "description": "Connection to PostgreSQL or MySQL failed due to authentication or network configuration.",
        "causes": [
            "User credentials are incorrect",
            "pg_hba.conf doesn't allow connections from ADF's IP",
            "Encryption method mismatch",
            "Flexible server is in private access mode"
        ],
        "solutions": [
            "Verify username and password in the linked service",
            "Add ADF IP to the server's firewall rules",
            "Check encryption settings match the server configuration",
            "For private access: use Self-hosted IR within the same VNet"
        ],
        "prevention": [
            "Document authentication requirements for each database",
            "Use managed private endpoints for private access servers"
        ],
        "estimated_fix_time": "15-30 minutes",
        "documentation": ["https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-database-for-postgresql"]
    }
]


class VectorKnowledgeBase:
    """
    Semantic vector-based knowledge base using ChromaDB.
    Enables intelligent error matching beyond simple regex patterns.
    """

    def __init__(self, persist_dir: str = None):
        if persist_dir is None:
            persist_dir = str(Path(__file__).parent.parent / "knowledge" / "chromadb")

        os.makedirs(persist_dir, exist_ok=True)
        self.persist_dir = persist_dir

        # Initialize ChromaDB with persistent storage
        self.client = chromadb.PersistentClient(path=persist_dir)

        # Create or get collection
        self.collection = self.client.get_or_create_collection(
            name="adf_errors",
            metadata={"hnsw:space": "cosine"}
        )

        # Populate if empty
        if self.collection.count() == 0:
            self._populate_knowledge_base()
            logger.info(f"Populated vector KB with {self.collection.count()} entries")
        else:
            logger.info(f"Loaded vector KB with {self.collection.count()} existing entries")

    def _populate_knowledge_base(self):
        """Populate the vector knowledge base with ADF error entries."""
        documents = []
        metadatas = []
        ids = []

        for entry in ADF_ERROR_KNOWLEDGE:
            # Create a rich text document for embedding
            doc_text = (
                f"Error: {entry['title']}. "
                f"Category: {entry['category']}. "
                f"Severity: {entry['severity']}. "
                f"Description: {entry['description']} "
                f"Common causes: {'. '.join(entry['causes'])}. "
                f"Solutions: {'. '.join(entry['solutions'])}. "
                f"Error patterns: {entry['error_pattern']}"
            )
            documents.append(doc_text)
            metadatas.append({
                "id": entry["id"],
                "category": entry["category"],
                "severity": entry["severity"],
                "title": entry["title"],
                "estimated_fix_time": entry.get("estimated_fix_time", "unknown"),
            })
            ids.append(entry["id"])

        # Add to ChromaDB (auto-embeds with default model)
        self.collection.add(
            documents=documents,
            metadatas=metadatas,
            ids=ids,
        )
        logger.info(f"Added {len(documents)} error patterns to vector KB")

    def search(self, error_message: str, n_results: int = 5) -> list:
        """
        Search for similar errors using semantic similarity.
        Returns top N matching knowledge entries with scores.
        """
        results = self.collection.query(
            query_texts=[error_message],
            n_results=min(n_results, self.collection.count()),
        )

        matches = []
        if results and results["ids"] and results["ids"][0]:
            for i, doc_id in enumerate(results["ids"][0]):
                # Find the full entry
                entry = next(
                    (e for e in ADF_ERROR_KNOWLEDGE if e["id"] == doc_id), None
                )
                if entry:
                    distance = results["distances"][0][i] if results.get("distances") else 0
                    similarity = 1 - distance  # Convert distance to similarity
                    matches.append({
                        "entry": entry,
                        "similarity": round(similarity, 4),
                        "distance": round(distance, 4),
                    })

        return matches

    def get_enrichment(self, error_message: str) -> dict:
        """
        Get enrichment data for an error message using semantic search.
        Returns the best matching entry with full details.
        """
        if not error_message:
            return {"pattern_matched": False}

        matches = self.search(error_message, n_results=3)

        if not matches:
            return {"pattern_matched": False}

        best = matches[0]
        entry = best["entry"]

        # Only consider it a match if similarity is above threshold
        if best["similarity"] < 0.3:
            return {"pattern_matched": False, "closest_match": entry["title"]}

        return {
            "pattern_matched": True,
            "match_confidence": best["similarity"],
            "error_entry": entry,
            "category": entry["category"],
            "severity": entry["severity"],
            "known_causes": entry["causes"],
            "known_solutions": entry["solutions"],
            "prevention": entry.get("prevention", []),
            "estimated_fix_time": entry.get("estimated_fix_time", "unknown"),
            "documentation_links": entry.get("documentation", []),
            "runbook": {
                "title": f"Troubleshoot: {entry['title']}",
                "steps": entry["solutions"][:8],
                "estimated_time": entry.get("estimated_fix_time", "15-30 minutes"),
                "prevention": entry.get("prevention", []),
            },
            "similar_errors": [
                {"title": m["entry"]["title"], "similarity": m["similarity"]}
                for m in matches[1:3]
            ],
        }

    def get_all_entries(self) -> list:
        """Return all knowledge base entries."""
        return ADF_ERROR_KNOWLEDGE

    def get_entry_by_id(self, entry_id: str) -> Optional[dict]:
        """Get a specific entry by ID."""
        return next(
            (e for e in ADF_ERROR_KNOWLEDGE if e["id"] == entry_id), None
        )

    def get_entries_by_category(self, category: str) -> list:
        """Get all entries for a specific category."""
        return [e for e in ADF_ERROR_KNOWLEDGE if e["category"] == category]

    def get_stats(self) -> dict:
        """Get knowledge base statistics."""
        categories = {}
        for entry in ADF_ERROR_KNOWLEDGE:
            cat = entry["category"]
            categories[cat] = categories.get(cat, 0) + 1

        return {
            "total_entries": len(ADF_ERROR_KNOWLEDGE),
            "vector_count": self.collection.count(),
            "categories": categories,
        }
