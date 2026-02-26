"""
ADF Pipeline Debugger - Web Dashboard
Flask-based dashboard for monitoring and analyzing ADF pipeline failures.
"""
import sys
import json
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta

from flask import Flask, render_template_string, jsonify, request
from azure.mgmt.datafactory.models import RunQueryFilterOperand, RunQueryFilterOperator, RunFilterParameters

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import config as app_config # Rename to avoid conflict with the new config object
from adf_debugger.utils import setup_logging, format_duration, format_timestamp, time_ago, severity_emoji, error_category_emoji
from adf_debugger.adf_client import ADFClient
from adf_debugger.knowledge_base import KnowledgeBase
from adf_debugger.vector_knowledge_base import VectorKnowledgeBase
from adf_debugger.error_analyzer import ErrorAnalyzer
from adf_debugger.data_quality import DataQualityChecker
from adf_debugger.report_builder import ReportBuilder
import importlib.util
spec = importlib.util.spec_from_file_location("config", str(Path(__file__).parent / "config.py"))
config_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config_module)

# Initialize components
config = config_module
adf_client = ADFClient(
    subscription_id=config.azure.SUBSCRIPTION_ID,
    resource_group=config.azure.RESOURCE_GROUP,
    factory_name=config.azure.DATA_FACTORY_NAME,
    tenant_id=config.azure.TENANT_ID,
    client_id=config.azure.CLIENT_ID,
    client_secret=config.azure.CLIENT_SECRET,
)
knowledge_base = KnowledgeBase()
try:
    vector_kb = VectorKnowledgeBase()
    vector_kb_available = True
except Exception as e:
    vector_kb_available = False
    print(f"Warning: Vector KB not available: {e}")

logger = setup_logging(config.app.LOG_LEVEL)

app = Flask(__name__)
app.secret_key = config.dashboard.SECRET_KEY

# ============================================================
# Dashboard HTML Template (embedded for single-file simplicity)
# ============================================================
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ADF Pipeline Debugger</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

        :root {
            --bg-primary: #0a0b10;
            --bg-secondary: #12141d;
            --bg-card: #1a1d27;
            --bg-card-hover: #1e2233;
            --bg-elevated: #252938;
            --border: #2d3141;
            --text-primary: #f0f1f4;
            --text-secondary: #c4c7d4;
            --text-muted: #8b8fa3;
            --text-dim: #6b6f82;
            --accent-red: #ef4444;
            --accent-orange: #f59e0b;
            --accent-green: #22c55e;
            --accent-blue: #3b82f6;
            --accent-purple: #a78bfa;
            --accent-cyan: #06b6d4;
            --shadow: 0 4px 24px rgba(0,0,0,0.3);
            --radius: 12px;
        }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 10vh;
            line-height: 1.5;
        }

        /* ===== HEADER ===== */
        .header {
            background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-card) 100%);
            border-bottom: 1px solid var(--border);
            padding: 20px 32px;
            position: sticky;
            top: 0;
            z-index: 100;
            backdrop-filter: blur(20px);
        }
        .header-content {
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .header h1 {
            font-size: 20px;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-purple), var(--accent-cyan));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .header h1 span { font-size: 24px; -webkit-text-fill-color: initial; }
        .header-meta {
            display: flex;
            align-items: center;
            gap: 20px;
            font-size: 13px;
            color: var(--text-muted);
        }
        .status-dot {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 6px;
            animation: pulse 2s infinite;
        }
        .status-dot.online { background: var(--accent-green); }
        .status-dot.offline { background: var(--accent-red); }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        /* ===== LAYOUT ===== */
        .main {
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px 32px;
        }

        /* ===== STATS CARDS ===== */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 28px;
        }
        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 20px 24px;
            transition: all 0.2s ease;
        }
        .stat-card:hover {
            background: var(--bg-card-hover);
            transform: translateY(-2px);
            box-shadow: var(--shadow);
        }
        .stat-label {
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
            margin-bottom: 8px;
        }
        .stat-value {
            font-size: 32px;
            font-weight: 800;
            letter-spacing: -1px;
        }
        .stat-value.red { color: var(--accent-red); }
        .stat-value.orange { color: var(--accent-orange); }
        .stat-value.green { color: var(--accent-green); }
        .stat-value.blue { color: var(--accent-blue); }
        .stat-sub {
            font-size: 12px;
            color: var(--text-dim);
            margin-top: 4px;
        }

        /* ===== NAV TABS ===== */
        .tabs {
            display: flex;
            gap: 4px;
            margin-bottom: 24px;
            background: var(--bg-secondary);
            border-radius: 10px;
            padding: 4px;
            border: 1px solid var(--border);
        }
        .tab {
            padding: 10px 20px;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 500;
            color: var(--text-muted);
            cursor: pointer;
            transition: all 0.2s;
            border: none;
            background: none;
        }
        .tab:hover { color: var(--text-primary); background: var(--bg-card); }
        .tab.active {
            color: var(--text-primary);
            background: var(--bg-card);
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        }

        /* ===== FAILURES TABLE ===== */
        .section { margin-bottom: 32px; }
        .section-title {
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .failure-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 20px 24px;
            margin-bottom: 12px;
            cursor: pointer;
            transition: all 0.2s ease;
            border-left: 4px solid var(--accent-red);
        }
        .failure-card:hover {
            background: var(--bg-card-hover);
            transform: translateX(4px);
            box-shadow: var(--shadow);
        }
        .failure-card.severity-critical { border-left-color: var(--accent-red); }
        .failure-card.severity-high { border-left-color: #f97316; }
        .failure-card.severity-medium { border-left-color: var(--accent-orange); }
        .failure-card.severity-low { border-left-color: var(--accent-green); }

        .failure-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 10px;
        }
        .failure-pipeline {
            font-size: 16px;
            font-weight: 600;
            color: var(--text-primary);
        }
        .failure-time {
            font-size: 12px;
            color: var(--text-muted);
        }
        .failure-error {
            font-size: 13px;
            color: var(--text-secondary);
            margin-bottom: 10px;
            line-height: 1.4;
        }
        .failure-meta {
            display: flex;
            gap: 16px;
            font-size: 12px;
            color: var(--text-dim);
        }
        .failure-meta span { display: flex; align-items: center; gap: 4px; }

        .badge {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }
        .badge-critical { background: rgba(239,68,68,0.15); color: var(--accent-red); }
        .badge-high { background: rgba(249,115,22,0.15); color: #f97316; }
        .badge-medium { background: rgba(245,158,11,0.15); color: var(--accent-orange); }
        .badge-low { background: rgba(34,197,94,0.15); color: var(--accent-green); }

        /* ===== ANALYSIS PANEL ===== */
        .analysis-panel {
            display: none;
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            margin-top: 12px;
            overflow: hidden;
        }
        .analysis-panel.open { display: block; }

        .analysis-section {
            padding: 20px 24px;
            border-bottom: 1px solid var(--border);
        }
        .analysis-section:last-child { border-bottom: none; }
        .analysis-section-title {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 10px;
            color: var(--text-primary);
        }
        .analysis-content {
            font-size: 14px;
            color: var(--text-secondary);
            line-height: 1.6;
        }

        .solution-card {
            background: var(--bg-card);
            border-radius: 8px;
            padding: 14px 18px;
            margin-bottom: 8px;
            border: 1px solid var(--border);
        }
        .solution-title {
            font-size: 14px;
            font-weight: 600;
            color: var(--accent-green);
            margin-bottom: 6px;
        }
        .solution-steps {
            list-style: none;
            padding: 0;
        }
        .solution-steps li {
            font-size: 13px;
            color: var(--text-secondary);
            padding: 3px 0;
            padding-left: 16px;
            position: relative;
        }
        .solution-steps li::before {
            content: '→';
            position: absolute;
            left: 0;
            color: var(--accent-green);
        }

        /* ===== ANALYZE FORM ===== */
        .analyze-form {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 24px;
            margin-bottom: 24px;
        }
        .form-group { margin-bottom: 16px; }
        .form-label {
            font-size: 13px;
            font-weight: 500;
            color: var(--text-muted);
            margin-bottom: 6px;
            display: block;
        }
        .form-input, .form-textarea {
            width: 100%;
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 10px 14px;
            font-size: 14px;
            color: var(--text-primary);
            font-family: inherit;
        }
        .form-textarea { min-height: 100px; resize: vertical; }
        .form-input:focus, .form-textarea:focus {
            outline: none;
            border-color: var(--accent-purple);
            box-shadow: 0 0 0 3px rgba(167,139,250,0.1);
        }
        .btn {
            padding: 10px 24px;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            border: none;
            transition: all 0.2s;
        }
        .btn-primary {
            background: linear-gradient(135deg, var(--accent-purple), var(--accent-blue));
            color: white;
        }
        .btn-primary:hover {
            opacity: 0.9;
            transform: translateY(-1px);
        }
        .btn-primary:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }

        /* ===== KNOWLEDGE BASE ===== */
        .kb-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
            gap: 16px;
        }
        .kb-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            padding: 20px;
            transition: all 0.2s;
        }
        .kb-card:hover {
            background: var(--bg-card-hover);
            box-shadow: var(--shadow);
        }
        .kb-title {
            font-size: 15px;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 6px;
        }
        .kb-desc {
            font-size: 13px;
            color: var(--text-muted);
            margin-bottom: 10px;
        }
        .kb-category {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 6px;
            font-size: 11px;
            background: rgba(167,139,250,0.1);
            color: var(--accent-purple);
        }

        /* ===== LOADING ===== */
        .spinner {
            display: inline-block;
            width: 20px;
            height: 20px;
            border: 2px solid var(--border);
            border-top-color: var(--accent-purple);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }
        @keyframes spin { to { transform: rotate(360deg); } }

        .loading-overlay {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(10,11,16,0.8);
            z-index: 1000;
            justify-content: center;
            align-items: center;
            flex-direction: column;
            gap: 16px;
        }
        .loading-overlay.show { display: flex; }
        .loading-text { color: var(--text-muted); font-size: 14px; }

        /* ===== TAB CONTENT ===== */
        .tab-content { display: none; }
        .tab-content.active { display: block; }

        /* ===== EMPTY STATE ===== */
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: var(--text-dim);
        }
        .empty-state .icon { font-size: 48px; margin-bottom: 16px; }
        .empty-state p { font-size: 15px; }

        /* ===== RESPONSIVE ===== */
        @media (max-width: 768px) {
            .header { padding: 16px 20px; }
            .main { padding: 16px 20px; }
            .stats-grid { grid-template-columns: repeat(2, 1fr); }
            .kb-grid { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>

    <!-- HEADER -->
    <header class="header">
        <div class="header-content">
            <h1><span>🔧</span> ADF Pipeline Debugger</h1>
            <div class="header-meta">
                <span>
                    <span class="status-dot" id="statusDot"></span>
                    <span id="connectionStatus">Checking...</span>
                </span>
                <span id="factoryName">{{ factory_name }}</span>
            </div>
        </div>
    </header>

    <!-- MAIN CONTENT -->
    <main class="main">
        <!-- STATS -->
        <div class="stats-grid" id="statsGrid">
            <div class="stat-card">
                <div class="stat-label">Total Failures (24h)</div>
                <div class="stat-value red" id="statFailures">-</div>
                <div class="stat-sub">Pipeline runs</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Critical</div>
                <div class="stat-value orange" id="statCritical">-</div>
                <div class="stat-sub">Need immediate attention</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Analyzed</div>
                <div class="stat-value blue" id="statAnalyzed">0</div>
                <div class="stat-sub">AI-powered diagnostics</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Knowledge Base</div>
                <div class="stat-value green" id="statKB">-</div>
                <div class="stat-sub">Error patterns loaded</div>
            </div>
        </div>

        <!-- TABS -->
        <div class="tabs">
            <button class="tab active" onclick="switchTab('failures')">🔴 Failures</button>
            <button class="tab" onclick="switchTab('analyze')">🔬 Analyze</button>
            <button class="tab" onclick="switchTab('knowledge')">📚 Knowledge Base</button>
            <button class="tab" onclick="switchTab('settings')">⚙️ Settings</button>
        </div>

        <!-- TAB: FAILURES -->
        <div class="tab-content active" id="tab-failures">
            <div class="section">
                <div class="section-title">
                    🔴 Recent Pipeline Failures
                    <button class="btn" style="margin-left: auto; font-size: 12px; padding: 6px 14px; background: var(--bg-elevated); color: var(--text-muted);" onclick="loadFailures()">↻ Refresh</button>
                </div>
                <div id="failuresList">
                    <div class="empty-state">
                        <div class="icon">🔍</div>
                        <p>Loading pipeline failures...</p>
                    </div>
                </div>
            </div>
        </div>

        <!-- TAB: ANALYZE -->
        <div class="tab-content" id="tab-analyze">
            <div class="section">
                <div class="section-title">🔬 Quick Error Analysis</div>
                <div class="analyze-form">
                    <div class="form-group">
                        <label class="form-label">Pipeline Name (optional)</label>
                        <input class="form-input" id="analyzePipeline" placeholder="e.g., ETL_Sales_Daily">
                    </div>
                    <div class="form-group">
                        <label class="form-label">Error Message</label>
                        <textarea class="form-textarea" id="analyzeError" placeholder="Paste the error message here..."></textarea>
                    </div>
                    <button class="btn btn-primary" id="analyzeBtn" onclick="analyzeError()">
                        🔬 Analyze with AI
                    </button>
                </div>
                <div id="analyzeResults"></div>
            </div>
        </div>

        <!-- TAB: KNOWLEDGE BASE -->
        <div class="tab-content" id="tab-knowledge">
            <div class="section">
                <div class="section-title">📚 Error Knowledge Base</div>
                <div class="form-group" style="margin-bottom: 20px;">
                    <input class="form-input" id="kbSearch" placeholder="Search error patterns..." oninput="filterKB()">
                </div>
                <div class="kb-grid" id="kbGrid"></div>
            </div>
        </div>

        <!-- TAB: SETTINGS -->
        <div class="tab-content" id="tab-settings">
            <div class="section">
                <div class="section-title">⚙️ Configuration</div>
                <div class="analyze-form">
                    <div class="form-group">
                        <label class="form-label">Azure Data Factory</label>
                        <input class="form-input" value="{{ factory_name }}" readonly>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Resource Group</label>
                        <input class="form-input" value="{{ resource_group }}" readonly>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Lookback Hours</label>
                        <input class="form-input" type="number" value="{{ lookback_hours }}" id="lookbackHours">
                    </div>
                    <button class="btn btn-primary" onclick="sendTestEmail()">📧 Send Test Email</button>
                </div>
            </div>
        </div>
    </main>

    <!-- Loading Overlay -->
    <div class="loading-overlay" id="loadingOverlay">
        <div class="spinner" style="width: 40px; height: 40px; border-width: 3px;"></div>
        <div class="loading-text" id="loadingText">Analyzing with Gemini AI...</div>
    </div>

    <script>
        // ===== STATE =====
        let allKBEntries = [];
        let analyzedCount = 0;
        let vectorKbAvailable = {{ 'true' if vector_kb_available else 'false' }};

        // ===== TAB SWITCHING =====
        function switchTab(tabId) {
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.getElementById('tab-' + tabId).classList.add('active');
            event.target.classList.add('active');
        }

        // ===== LOAD FAILURES =====
        async function loadFailures() {
            const list = document.getElementById('failuresList');
            list.innerHTML = '<div class="empty-state"><div class="spinner"></div><p style="margin-top:12px">Fetching failures from ADF...</p></div>';

            try {
                const resp = await fetch('/api/failures');
                const data = await resp.json();

                if (data.error) {
                    list.innerHTML = `<div class="empty-state"><div class="icon">⚠️</div><p>${data.error}</p><p style="font-size:12px;margin-top:8px">Check your Azure credentials in .env</p></div>`;
                    return;
                }

                document.getElementById('statFailures').textContent = data.failures.length;

                if (data.failures.length === 0) {
                    list.innerHTML = '<div class="empty-state"><div class="icon">✅</div><p>No pipeline failures found! All clear.</p></div>';
                    return;
                }

                list.innerHTML = data.failures.map((f, i) => `
                    <div class="failure-card" onclick="toggleAnalysis(${i}, '${f.run_id}')">
                        <div class="failure-header">
                            <div class="failure-pipeline">${f.pipeline_name}</div>
                            <div class="failure-time">${f.run_start || 'N/A'}</div>
                        </div>
                        <div class="failure-error">${(f.message || 'No error message').substring(0, 200)}${(f.message || '').length > 200 ? '...' : ''}</div>
                        <div class="failure-meta">
                            <span>🆔 ${(f.run_id || '').substring(0, 12)}...</span>
                            <span>⏱️ ${f.duration || 'N/A'}</span>
                            <span>📋 Click to analyze</span>
                        </div>
                        <div class="analysis-panel" id="analysis-${i}"></div>
                    </div>
                `).join('');
            } catch (err) {
                list.innerHTML = `<div class="empty-state"><div class="icon">❌</div><p>Failed to load: ${err.message}</p></div>`;
            }
        }

        // ===== TOGGLE ANALYSIS =====
        async function toggleAnalysis(index, runId) {
            const panel = document.getElementById('analysis-' + index);
            if (panel.classList.contains('open')) {
                panel.classList.remove('open');
                return;
            }

            panel.innerHTML = '<div style="padding:20px;text-align:center"><div class="spinner"></div><p style="margin-top:10px;color:var(--text-muted);font-size:13px">Running AI analysis...</p></div>';
            panel.classList.add('open');

            try {
                const resp = await fetch('/api/analyze/' + runId);
                const data = await resp.json();

                if (data.error) {
                    panel.innerHTML = `<div class="analysis-section"><p style="color:var(--accent-red)">${data.error}</p></div>`;
                    return;
                }

                analyzedCount++;
                document.getElementById('statAnalyzed').textContent = analyzedCount;
                renderAnalysis(panel, data);
            } catch (err) {
                panel.innerHTML = `<div class="analysis-section"><p style="color:var(--accent-red)">Analysis failed: ${err.message}</p></div>`;
            }
        }

        // ===== RENDER ANALYSIS RESULTS =====
        function renderAnalysis(panel, data) {
            const a = data.analysis || data;
            const q = data.quality_checks || {};

            let html = '';

            // What went wrong
            html += `<div class="analysis-section">
                <div class="analysis-section-title">🔴 What Went Wrong</div>
                <div class="analysis-content" style="border-left:3px solid var(--accent-red);padding-left:14px">
                    ${a.plain_english_error || 'No explanation available'}
                </div>
            </div>`;

            // Root cause
            html += `<div class="analysis-section">
                <div class="analysis-section-title">🔍 Root Cause</div>
                <div class="analysis-content" style="border-left:3px solid var(--accent-orange);padding-left:14px">
                    ${a.root_cause || 'Unable to determine'}
                </div>
            </div>`;

            // Solutions
            if (a.solutions && a.solutions.length > 0) {
                html += `<div class="analysis-section">
                    <div class="analysis-section-title">💡 Suggested Solutions</div>`;
                a.solutions.forEach((s, i) => {
                    html += `<div class="solution-card">
                        <div class="solution-title">${i+1}. ${s.title || 'Solution'}</div>
                        <div style="font-size:12px;color:var(--text-dim);margin-bottom:6px">⏱️ ${s.estimated_time || 'N/A'} | Likelihood: ${s.likelihood || 'N/A'}</div>
                        <ul class="solution-steps">
                            ${(s.steps || []).map(step => `<li>${step}</li>`).join('')}
                        </ul>
                    </div>`;
                });
                html += `</div>`;
            }

            // Quality checks
            if (q.issues_found && q.issues_found.length > 0) {
                html += `<div class="analysis-section">
                    <div class="analysis-section-title">📊 Data Quality Findings</div>
                    ${q.issues_found.map(i => `<p style="font-size:13px;color:var(--text-secondary);margin:4px 0">⚠️ ${i}</p>`).join('')}
                </div>`;
            }

            // Preventive measures
            if (a.preventive_measures && a.preventive_measures.length > 0) {
                html += `<div class="analysis-section">
                    <div class="analysis-section-title">🛡️ Preventive Measures</div>
                    ${a.preventive_measures.map(m => `<p style="font-size:13px;color:var(--text-secondary);margin:4px 0">✓ ${m}</p>`).join('')}
                </div>`;
            }

            // Send email button
            html += `<div class="analysis-section" style="text-align:center">
                <button class="btn btn-primary" onclick="sendReport('${a.run_id}', event)" style="font-size:13px">
                    📧 Send Diagnostic Email
                </button>
            </div>`;

            panel.innerHTML = html;
        }

        // ===== ANALYZE ERROR (QUICK) =====
        async function analyzeError() {
            const pipeline = document.getElementById('analyzePipeline').value;
            const error = document.getElementById('analyzeError').value;

            if (!error.trim()) {
                alert('Please enter an error message');
                return;
            }

            const btn = document.getElementById('analyzeBtn');
            btn.disabled = true;
            btn.textContent = '⏳ Analyzing...';

            const results = document.getElementById('analyzeResults');
            results.innerHTML = '<div style="text-align:center;padding:20px"><div class="spinner"></div><p style="margin-top:10px;color:var(--text-muted)">Running AI analysis with Gemini...</p></div>';

            try {
                const resp = await fetch('/api/quick-analyze', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ pipeline_name: pipeline, error_message: error })
                });
                const data = await resp.json();

                analyzedCount++;
                document.getElementById('statAnalyzed').textContent = analyzedCount;

                const panel = document.createElement('div');
                panel.className = 'analysis-panel open';
                renderAnalysis(panel, data);
                results.innerHTML = '';
                results.appendChild(panel);
            } catch (err) {
                results.innerHTML = `<div style="color:var(--accent-red);padding:16px">Error: ${err.message}</div>`;
            }

            btn.disabled = false;
            btn.textContent = '🔬 Analyze with AI';
        }

        // ===== LOAD KNOWLEDGE BASE =====
        async function loadKB() {
            try {
                const resp = await fetch('/api/knowledge-base');
                const data = await resp.json();
                
                if (vectorKbAvailable) {
                    allKBEntries = data.entries || [];
                    document.getElementById('statKB').textContent = allKBEntries.length;
                    renderKB(allKBEntries);
                } else {
                    // Legacy KB structure
                    allKBEntries = [...(data.errors || []), ...(data.runbooks || [])];
                    document.getElementById('statKB').textContent = allKBEntries.length;
                    renderKB(allKBEntries);
                }
            } catch (err) {
                console.error('Failed to load KB:', err);
            }
        }

        function renderKB(entries) {
            const grid = document.getElementById('kbGrid');
            grid.innerHTML = entries.map(e => `
                <div class="kb-card">
                    <div class="kb-title">${e.title || e.name}</div>
                    <div class="kb-desc">${e.description || e.summary}</div>
                    <span class="kb-category">${e.category || 'General'}</span>
                    ${e.severity ? `<span class="badge badge-${e.severity.toLowerCase()}" style="margin-left:6px">${e.severity}</span>` : ''}
                </div>
            `).join('');
        }

        function filterKB() {
            const query = document.getElementById('kbSearch').value.toLowerCase();
            
            if (vectorKbAvailable && query.length > 2) { // Only use vector search for longer queries
                fetch(`/api/vector-search?q=${encodeURIComponent(query)}`)
                    .then(res => res.json())
                    .then(data => {
                        if (data.matches) {
                            renderKB(data.matches.map(m => m.metadata));
                        }
                    })
                    .catch(err => console.error('Vector search failed:', err));
            } else {
                const filtered = allKBEntries.filter(e =>
                    (e.title && e.title.toLowerCase().includes(query)) ||
                    (e.name && e.name.toLowerCase().includes(query)) ||
                    (e.description && e.description.toLowerCase().includes(query)) ||
                    (e.summary && e.summary.toLowerCase().includes(query)) ||
                    (e.category && e.category.toLowerCase().includes(query))
                );
                renderKB(filtered);
            }
        }

        // ===== CHECK CONNECTION =====
        async function checkConnection() {
            try {
                const resp = await fetch('/api/status');
                const data = await resp.json();
                const dot = document.getElementById('statusDot');
                const status = document.getElementById('connectionStatus');
                if (data.connected) {
                    dot.classList.add('online');
                    dot.classList.remove('offline');
                    status.textContent = 'Connected';
                } else {
                    dot.classList.add('offline');
                    dot.classList.remove('online');
                    status.textContent = 'Disconnected';
                }
            } catch {
                document.getElementById('statusDot').classList.add('offline');
                document.getElementById('connectionStatus').textContent = 'Offline';
            }
        }

        // ===== SEND REPORT EMAIL =====
        async function sendReport(runId, event) {
            event.stopPropagation();
            try {
                const resp = await fetch('/api/send-report/' + runId, { method: 'POST' });
                const data = await resp.json();
                alert(data.success ? '✅ Email sent!' : '❌ ' + (data.error || 'Failed'));
            } catch (err) {
                alert('Failed: ' + err.message);
            }
        }

        // ===== SEND TEST EMAIL =====
        async function sendTestEmail() {
            try {
                const resp = await fetch('/api/send-test-email', { method: 'POST' });
                const data = await resp.json();
                alert(data.success ? '✅ Test email sent!' : '❌ ' + (data.error || 'Failed'));
            } catch (err) {
                alert('Failed: ' + err.message);
            }
        }

        // ===== INITIALIZE =====
        checkConnection();
        loadFailures();
        loadKB();
        // Auto-refresh every 5 minutes
        setInterval(loadFailures, 300000);
        setInterval(checkConnection, 60000);
    </script>
</body>
</html>
"""


# ============================================================
# API Routes
# ============================================================

@app.route("/")
def dashboard():
    """Render the main dashboard."""
    return render_template_string(
        DASHBOARD_HTML,
        factory_name=config.azure.DATA_FACTORY_NAME or "Not configured",
        resource_group=config.azure.RESOURCE_GROUP or "Not configured",
        lookback_hours=config.app.LOOKBACK_HOURS,
        vector_kb_available=vector_kb_available,
    )


@app.route("/api/status")
def api_status():
    """Check ADF connection status."""
    try:
        result = adf_client.test_connection()
        return jsonify(result)
    except Exception as e:
        return jsonify({"connected": False, "error": str(e)})


@app.route("/api/failures")
def api_failures():
    """Get recent pipeline failures."""
    try:
        hours = request.args.get("hours", config.app.LOOKBACK_HOURS, type=int)
        runs = adf_client.get_failed_pipeline_runs(hours_back=hours)

        failures = []
        for run in runs:
            failures.append({
                "pipeline_name": run.pipeline_name,
                "run_id": run.run_id,
                "status": run.status,
                "message": getattr(run, "message", None),
                "run_start": format_timestamp(run.run_start),
                "duration": format_duration((run.duration_in_ms or 0) / 1000),
            })

        return jsonify({"failures": failures})
    except Exception as e:
        return jsonify({"failures": [], "error": str(e)})


@app.route("/api/analyze/<run_id>")
def api_analyze(run_id):
    """Run full analysis on a pipeline run."""
    try:
        error_details = adf_client.get_error_details(run_id)
        analyzer = ErrorAnalyzer(api_key=config.gemini.API_KEY, model=config.gemini.MODEL)
        quality_checker = DataQualityChecker(adf_client=adf_client)

        analysis = analyzer.analyze(error_details)
        quality_checks = quality_checker.run_checks(error_details)

        return jsonify({"analysis": analysis, "quality_checks": quality_checks})
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/quick-analyze", methods=["POST"])
def api_quick_analyze():
    """Quick analyze an error message."""
    try:
        data = request.json
        error_message = data.get("error_message", "")
        pipeline_name = data.get("pipeline_name", "Unknown")

        analyzer = ErrorAnalyzer(api_key=config.gemini.API_KEY, model=config.gemini.MODEL)
        analysis = analyzer.quick_analyze(error_message, pipeline_name=pipeline_name)

        # Run basic quality checks
        quality_checker = DataQualityChecker()
        error_details = {
            "pipeline_name": pipeline_name,
            "primary_error_message": error_message,
            "run_start": datetime.now(timezone.utc),
            "duration_ms": None,
            "total_activities": 0,
            "failed_activities": [],
            "succeeded_activities": [],
            "parameters": {},
            "primary_failure_type": "",
        }
        quality_checks = quality_checker.run_checks(error_details)

        return jsonify({"analysis": analysis, "quality_checks": quality_checks})
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/knowledge-base', methods=['GET'])
def api_knowledge_base():
    """API endpoint to get the knowledge base entries."""
    if vector_kb_available:
        stats = vector_kb.get_stats()
        entries = vector_kb.get_all_entries()
        return jsonify({
            'stats': stats,
            'entries': entries,
            'source': 'Vector KB (ChromaDB)'
        })
    else:
        return jsonify({
            'errors': knowledge_base.common_errors,
            'runbooks': knowledge_base.runbooks,
            'source': 'Legacy Regex KB'
        })

@app.route('/api/vector-search', methods=['GET'])
def api_vector_search():
    """API endpoint to perform semantic search on error patterns."""
    if not vector_kb_available:
        return jsonify({'error': 'Vector search is not available'}), 503
        
    query = request.args.get('q', '')
    if not query:
        return jsonify({'error': 'Query parameter "q" is required'}), 400
        
    try:
        matches = vector_kb.search(query, n_results=5)
        return jsonify({
            'query': query,
            'matches': matches
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/agent_search', methods=['POST'])
def api_agent_search():
    """Logic App AI Agent tool endpoint for semantic error search."""
    if not vector_kb_available:
        return jsonify({'error': 'Vector search is not available'}), 503
    
    data = request.json
    if not data or 'query' not in data:
        return jsonify({'error': 'Missing required "query" in JSON body'}), 400
        
    query = data['query']
    try:
        matches = vector_kb.search(query, n_results=3)
        # matches is a list of dicts returning {"id": ..., "score": ..., "metadata": {...}}
        results = []
        for match in matches:
            meta = match.get("metadata", {})
            results.append({
                "title": meta.get("title", "Unknown Error"),
                "description": meta.get("description", "No description available"),
                "solution": meta.get("solutions", meta.get("solution", "No solution provided")),
                "severity": meta.get("severity", "medium"),
            })
        return jsonify({
            'success': True,
            'query': query,
            'top_matches': results
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/openapi.json', methods=['GET'])
def api_openapi_json():
    """Serve the OpenAPI specification for Logic App Autonomous Agents."""
    spec_path = Path(__file__).parent / "logic_app_agent" / "openapi.json"
    if spec_path.exists():
        with open(spec_path, "r", encoding="utf-8") as f:
            return jsonify(json.load(f))
    return jsonify({"error": "openapi.json not found"}), 404

@app.route('/api/pipeline-history', methods=['GET'])
def api_pipeline_history():
    """API endpoint to get historical success/failure trends."""
    pipeline_name = request.args.get('pipeline')
    hours = int(request.args.get('hours', 24))
    
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        
        # Get all runs (not just failures)
        filters = []
        if pipeline_name:
            filters.append(
                RunQueryFilterOperand(
                    operand="PipelineName", # Use string literal for operand
                    operator="Equals", # Use string literal for operator
                    values=[pipeline_name]
                )
            )
            
        run_filter = RunFilterParameters(
            last_updated_after=start_time,
            last_updated_before=end_time,
            filters=filters if filters else None
        )
        
        runs = adf_client.client.pipeline_runs.query_by_factory(
            config.azure.RESOURCE_GROUP, # Use config.azure
            config.azure.DATA_FACTORY_NAME, # Use config.azure
            run_filter
        )
        
        history = []
        for run in runs.value:
            history.append({
                'run_id': run.run_id,
                'pipeline_name': run.pipeline_name,
                'status': run.status,
                'start': run.run_start.isoformat() if run.run_start else None,
                'end': run.run_end.isoformat() if run.run_end else None,
                'duration_ms': run.duration_in_ms
            })
            
        return jsonify({'history': history})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route("/api/send-report/<run_id>", methods=["POST"])
def api_send_report(run_id):
    """Analyze and send email report."""
    try:
        from adf_debugger.adf_client import ADFClient
        client = ADFClient(
            subscription_id=config.azure.SUBSCRIPTION_ID,
            resource_group=config.azure.RESOURCE_GROUP,
            factory_name=config.azure.DATA_FACTORY_NAME,
            tenant_id=config.azure.TENANT_ID,
            client_id=config.azure.CLIENT_ID,
            client_secret=config.azure.CLIENT_SECRET,
        )

        error_details = client.get_error_details(run_id)
        analyzer = ErrorAnalyzer(api_key=config.gemini.API_KEY, model=config.gemini.MODEL)
        quality_checker = DataQualityChecker(adf_client=client)
        report_builder = ReportBuilder(
            subscription_id=config.azure.SUBSCRIPTION_ID,
            resource_group=config.azure.RESOURCE_GROUP,
            factory_name=config.azure.DATA_FACTORY_NAME,
        )

        analysis = analyzer.analyze(error_details)
        quality_checks = quality_checker.run_checks(error_details)
        history = client.get_pipeline_history(error_details["pipeline_name"], count=5)
        report = report_builder.build_report(analysis, quality_checks, history)

        notifier = NotificationService(
            smtp_host=config.email.SMTP_HOST,
            smtp_port=config.email.SMTP_PORT,
            username=config.email.USERNAME,
            password=config.email.PASSWORD,
            from_address=config.email.FROM_ADDRESS,
        )
        success = notifier.send_diagnostic_report(
            report=report,
            to_addresses=config.email.TO_ADDRESSES,
        )

        return jsonify({"success": success})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/send-test-email", methods=["POST"])
def api_send_test_email():
    """Send a test email."""
    try:
        notifier = NotificationService(
            smtp_host=config.email.SMTP_HOST,
            smtp_port=config.email.SMTP_PORT,
            username=config.email.USERNAME,
            password=config.email.PASSWORD,
            from_address=config.email.FROM_ADDRESS,
        )
        if config.email.TO_ADDRESSES:
            success = notifier.send_test_email(config.email.TO_ADDRESSES[0])
            return jsonify({"success": success})
        return jsonify({"success": False, "error": "No EMAIL_TO configured"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# ============================================================
# Run
# ============================================================
if __name__ == "__main__":
    print("\n🔧 ADF Pipeline Debugger - Web Dashboard")
    print(f"   Factory: {config.azure.DATA_FACTORY_NAME or 'Not configured'}")
    print(f"   Dashboard: http://localhost:{config.dashboard.PORT}")
    print(f"   Press Ctrl+C to quit\n")

    app.run(
        host=config.dashboard.HOST,
        port=config.dashboard.PORT,
        debug=True,
    )
