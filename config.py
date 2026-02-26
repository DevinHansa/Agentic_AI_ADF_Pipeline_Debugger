"""
ADF Pipeline Debugger - Configuration Module
Loads settings from environment variables and .env file.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)


class AzureConfig:
    """Azure connection settings."""
    SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "")
    RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "")
    DATA_FACTORY_NAME = os.getenv("AZURE_DATA_FACTORY_NAME", "")
    TENANT_ID = os.getenv("AZURE_TENANT_ID", "")
    CLIENT_ID = os.getenv("AZURE_CLIENT_ID", "")
    CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", "")


class GeminiConfig:
    """Gemini AI settings."""
    API_KEY = os.getenv("GEMINI_API_KEY", "")
    MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")


class EmailConfig:
    """Email (Gmail SMTP) settings."""
    SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    USERNAME = os.getenv("SMTP_USERNAME", "")
    PASSWORD = os.getenv("SMTP_PASSWORD", "")
    FROM_ADDRESS = os.getenv("EMAIL_FROM", "")
    TO_ADDRESSES = [
        addr.strip()
        for addr in os.getenv("EMAIL_TO", "").split(",")
        if addr.strip()
    ]


class DashboardConfig:
    """Web dashboard settings."""
    HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    PORT = int(os.getenv("DASHBOARD_PORT", "5000"))
    SECRET_KEY = os.getenv("DASHBOARD_SECRET_KEY", "dev-secret-key")


class AppConfig:
    """Application-level settings."""
    LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    BASE_DIR = Path(__file__).parent
    KNOWLEDGE_DIR = BASE_DIR / "knowledge"
    TEMPLATES_DIR = BASE_DIR / "templates"


# Singleton instances
azure = AzureConfig()
gemini = GeminiConfig()
email = EmailConfig()
dashboard = DashboardConfig()
app = AppConfig()
