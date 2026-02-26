# 🔧 ADF Pipeline Debugger

**AI-powered Azure Data Factory pipeline failure diagnostics** — automatically detects, analyzes, and reports pipeline failures with human-friendly explanations, root cause analysis, and step-by-step solutions.

> 🚀 Built for data engineers who are tired of being woken at 3 AM with "pipeline failed, check logs."

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🤖 **AI Error Analysis** | Gemini AI provides human-friendly error explanations and root cause analysis |
| 🧠 **Vector Knowledge Base** | 30+ ADF error patterns with semantic search (ChromaDB + sentence-transformers) |
| ✅ **Fact-Checking Agent** | Verifies analysis accuracy before sending reports — confidence scoring |
| 📧 **Smart Email Alerts** | Rich HTML diagnostic emails sent on pipeline failure via Gmail SMTP |
| 🌐 **Web Dashboard** | Real-time monitoring dashboard hosted on Azure App Service |
| 📊 **Data Quality Checks** | Automated checks for timing, parameters, and failure patterns |
| ⚡ **Azure Monitor Alerts** | Real-time failure detection every 5 minutes |
| 🔍 **CLI Tool** | Command-line interface for on-demand debugging and analysis |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Azure Data Factory                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │Pipeline 1│ │Pipeline 2│ │Pipeline 3│ │Pipeline N│      │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘      │
│       └─────────┬──┴────────────┴──────────┬──┘            │
└─────────────────┼──────────────────────────┼───────────────┘
                  │ Pipeline Failure          │
                  ▼                          ▼
┌─────────────────────────┐   ┌─────────────────────────────┐
│   Azure Monitor Alert   │   │      ADF Debugger CLI       │
│  (Every 5 min check)    │   │  python cli.py debug <id>   │
└────────┬────────────────┘   └──────────┬──────────────────┘
         │                               │
         ▼                               ▼
┌────────────────────────────────────────────────────────────┐
│                   Error Analysis Pipeline                   │
│                                                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │ Regex KB │→ │Vector KB │→ │Gemini AI │→ │Fact Check│  │
│  │(16 rules)│  │(30 docs) │  │(Analysis)│  │(Verify)  │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
└────────────────────────┬───────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  HTML Email  │ │  Dashboard   │ │  CLI Report  │
│  (Gmail)     │ │  (Azure App) │ │  (Terminal)  │
└──────────────┘ └──────────────┘ └──────────────┘
```

### Analysis Pipeline

1. **Regex KB** — Pattern matches against 16 known error rules
2. **Vector KB** — Semantic search across 30 Azure-documented error patterns using ChromaDB
3. **Gemini AI** — Deep AI analysis with context-aware solutions
4. **Fact-Checker** — Cross-references AI output against KB for accuracy verification

---

## 📂 Project Structure

```
ADF_Pipeline_Debugger/
├── adf_debugger/                # Core modules
│   ├── adf_client.py            # Azure SDK wrapper for ADF
│   ├── error_analyzer.py        # AI analysis pipeline
│   ├── vector_knowledge_base.py # ChromaDB semantic search (30 patterns)
│   ├── knowledge_base.py        # Regex-based pattern matching (16 rules)
│   ├── fact_checker.py          # AI fact-checking agent
│   ├── data_quality.py          # Data quality checks
│   ├── report_builder.py        # HTML/text report generation
│   ├── notification.py          # Gmail SMTP email service
│   └── utils.py                 # Helper utilities
├── knowledge/                   # Knowledge base data
│   ├── common_errors.json       # 16 regex error patterns
│   ├── runbooks.json            # 10 step-by-step troubleshooting guides
│   └── chromadb/                # Vector DB storage (auto-generated)
├── templates/
│   └── diagnostic_email.html    # Dark-themed HTML email template
├── azure_function/              # Azure Function App code
│   ├── function_app.py          # Alert webhook + timer trigger
│   ├── host.json                # Function host config
│   └── requirements.txt         # Function dependencies
├── test_pipelines/              # ADF test pipeline definitions
├── tests/                       # Test fixtures & mock data
├── cli.py                       # Command-line interface
├── dashboard.py                 # Flask web dashboard
├── config.py                    # Configuration loader
├── requirements.txt             # Python dependencies
└── .env.example                 # Environment variable template
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.12+
- Azure subscription with ADF instance
- Gemini API key
- Gmail account with App Password

### 1. Clone & Install

```bash
git clone https://github.com/YOUR_USERNAME/ADF_Pipeline_Debugger.git
cd ADF_Pipeline_Debugger
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required environment variables:
| Variable | Description |
|----------|-------------|
| `AZURE_SUBSCRIPTION_ID` | Azure subscription ID |
| `AZURE_RESOURCE_GROUP` | Resource group name |
| `AZURE_DATA_FACTORY_NAME` | ADF instance name |
| `AZURE_TENANT_ID` | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | Service principal client ID |
| `AZURE_CLIENT_SECRET` | Service principal secret |
| `GEMINI_API_KEY` | Google Gemini API key |
| `SMTP_USERNAME` | Gmail address |
| `SMTP_PASSWORD` | Gmail App Password |
| `EMAIL_TO` | Recipient email address(es) |

### 3. Test Connection

```bash
python cli.py test-connection
```

### 4. Run

```bash
# Check for pipeline failures
python cli.py failures --hours 24

# Analyze a specific failure
python cli.py debug <run-id> --save-html report.html --send-email

# Launch web dashboard
python dashboard.py

# Demo mode (no Azure needed)
python cli.py demo --scenario 0
```

---

## ☁️ Azure Deployment

### Deployed Resources

| Resource | Name | SKU |
|----------|------|-----|
| Web App | `adf-debugger-dashboard` | Free (F1) |
| Function App | `func-adf-debugger` | Basic (B1) |
| Monitor Alert | `adf-pipeline-failure-alert` | — |
| Action Group | `adf-debugger-actions` | — |
| Storage Account | `stadfdebuggersa` | Standard LRS |

### Deploy Dashboard

```bash
az webapp up --name adf-debugger-dashboard \
  --resource-group rg-adf-mads-mvp \
  --runtime "PYTHON:3.12"
```

### Configure App Settings

```bash
az webapp config appsettings set --name adf-debugger-dashboard \
  --resource-group rg-adf-mads-mvp \
  --settings AZURE_SUBSCRIPTION_ID="..." GEMINI_API_KEY="..." ...
```

---

## 📧 Email Report Example

The diagnostic email includes:
- ⚠️ Error severity badge (CRITICAL / HIGH / MEDIUM / LOW)
- 📝 Plain-English error explanation
- 🔍 Root cause analysis
- 💡 Step-by-step solutions with estimated fix times
- 🛡️ Preventive measures
- 📊 Data quality findings
- 🔗 Links to Azure documentation
- ✅ Fact-check confidence score

---

## 🔧 CLI Commands

| Command | Description |
|---------|-------------|
| `failures` | List recent pipeline failures |
| `debug <run-id>` | Full analysis of a specific failure |
| `history <pipeline>` | Show pipeline run history |
| `analyze <message>` | Quick analysis of an error message |
| `demo` | Demo mode with mock data |
| `test-connection` | Test Azure connectivity |
| `send-test-email` | Send a test email |
| `kb-stats` | Knowledge base statistics |

---

## 🧪 Test Pipelines

The following test pipelines are included for validation:

| Pipeline | Tests | Expected Error |
|----------|-------|----------------|
| `pl_sales_ingest_fail_404` | File not found | PathNotFound on ADLS Gen2 |
| `pl_test_connectivity` | Bad endpoint | DNS resolution failure |
| `pl_test_auth_fail` | Auth error | Unauthorized access |
| `pl_test_timeout` | HTTP 408 | Request timeout |
| `pl_test_bad_url` | Server error | HTTP 500 response |

---

## 📞 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-error-pattern`
3. Add new error patterns to `vector_knowledge_base.py`
4. Test with: `python cli.py demo`
5. Submit a pull request

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

**Built with ❤️ for data engineers who deserve better debugging tools.**
