"""
ADF Pipeline Debugger - AI-Powered Error Analyzer
Uses Gemini AI combined with the knowledge base and vector KB to provide intelligent error analysis.
Includes fact-checking verification before sending reports.
"""
import json
import logging
from typing import Optional

import google.generativeai as genai

from .knowledge_base import KnowledgeBase
from .vector_knowledge_base import VectorKnowledgeBase
from .fact_checker import FactCheckingAgent
from .utils import truncate_string

logger = logging.getLogger("adf_debugger.error_analyzer")

# System prompt for Gemini
SYSTEM_PROMPT = """You are an expert Azure Data Factory (ADF) pipeline debugger and data engineering specialist.
Your job is to analyze ADF pipeline failures and provide actionable diagnostic reports.

When given error details from a failed ADF pipeline, you must:

1. **Explain the error in plain English** - What went wrong, as if explaining to someone who just woke up at 3 AM.
2. **Identify the root cause** - Based on the error message, activity type, and context.
3. **Categorize the error** - Into one of: connectivity, authentication, permission, data_quality, timeout, resource, configuration, schema, missing_data, quota, unknown.
4. **Assess severity** - critical, high, medium, or low.
5. **Provide step-by-step solutions** - Ordered by likelihood of fixing the issue. Be specific with Azure portal navigation paths.
6. **Estimate fix time** - Rough estimate for each solution.
7. **Suggest preventive measures** - How to prevent this from happening again.
8. **Link to relevant documentation** - Microsoft docs links.

Always respond in valid JSON format with the following structure:
{
    "plain_english_error": "A clear, jargon-free explanation of what went wrong",
    "root_cause": "Most likely root cause based on the error details",
    "category": "error_category",
    "severity": "critical|high|medium|low",
    "solutions": [
        {
            "title": "Solution title",
            "steps": ["Step 1", "Step 2", "..."],
            "estimated_time": "X minutes",
            "likelihood": "high|medium|low"
        }
    ],
    "preventive_measures": ["Measure 1", "Measure 2"],
    "related_documentation": [
        {"title": "Doc title", "url": "https://..."}
    ],
    "additional_checks": ["Check 1", "Check 2"],
    "data_engineering_tips": "Any relevant data engineering best practice advice"
}
"""


class ErrorAnalyzer:
    """
    AI-powered error analyzer combining Gemini AI with a local knowledge base,
    vector semantic search (ChromaDB), and fact-checking verification.
    """

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        self.knowledge_base = KnowledgeBase()

        # Initialize vector knowledge base (ChromaDB)
        try:
            self.vector_kb = VectorKnowledgeBase()
            logger.info(f"Vector KB loaded: {self.vector_kb.get_stats()['total_entries']} entries")
        except Exception as e:
            logger.warning(f"Vector KB initialization failed: {e}")
            self.vector_kb = None

        # Initialize fact-checking agent
        try:
            self.fact_checker = FactCheckingAgent(api_key=api_key, model=model)
        except Exception as e:
            logger.warning(f"Fact-checker initialization failed: {e}")
            self.fact_checker = None

        # Configure Gemini
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            model_name=model,
            system_instruction=SYSTEM_PROMPT,
        )
        logger.info(f"Error Analyzer initialized with model: {model}")

    def analyze(self, error_details: dict) -> dict:
        """
        Perform comprehensive error analysis.
        Combines knowledge base pattern matching, vector semantic search,
        AI analysis, and fact-checking verification.

        Args:
            error_details: Dict from ADFClient.get_error_details()

        Returns:
            Complete diagnostic report as a dict.
        """
        primary_error = error_details.get("primary_error_message", "")

        # Step 1: Legacy KB enrichment (regex-based)
        kb_enrichment = self.knowledge_base.get_enrichment(primary_error)

        # Step 2: Vector KB enrichment (semantic search)
        vector_enrichment = {}
        vector_matches = []
        if self.vector_kb:
            try:
                vector_enrichment = self.vector_kb.get_enrichment(primary_error)
                vector_matches = self.vector_kb.search(primary_error, n_results=3)
                logger.info(f"Vector KB: matched={vector_enrichment.get('pattern_matched')}, "
                           f"confidence={vector_enrichment.get('match_confidence', 0):.2f}")
            except Exception as e:
                logger.warning(f"Vector KB search failed: {e}")

        # Step 3: Merge KB enrichments (vector KB takes priority if higher confidence)
        merged_kb = self._merge_kb_enrichments(kb_enrichment, vector_enrichment)

        # Step 4: AI analysis (with merged KB context)
        ai_analysis = self._get_ai_analysis(error_details, merged_kb)

        # Step 5: Merge all results
        result = self._merge_analysis(error_details, merged_kb, ai_analysis)

        # Step 6: Fact-checking verification
        if self.fact_checker:
            try:
                verification = self.fact_checker.verify(
                    error_details, result, vector_matches
                )
                result["fact_check"] = verification
                result["confidence_score"] = verification.get("confidence_score", 0.5)
                result["confidence_level"] = verification.get("confidence_level", "medium")
                logger.info(f"Fact-check: {verification.get('confidence_level')} "
                           f"({verification.get('confidence_score', 0):.2f})")
            except Exception as e:
                logger.warning(f"Fact-checking failed: {e}")
                result["fact_check"] = {"confidence_score": 0.6, "confidence_level": "medium"}
                result["confidence_score"] = 0.6
                result["confidence_level"] = "medium"
        else:
            result["confidence_score"] = 0.5
            result["confidence_level"] = "medium"

        # Step 7: Add vector KB similar errors
        if vector_matches:
            result["similar_errors"] = [
                {"title": m["entry"]["title"], "similarity": m["similarity"],
                 "category": m["entry"]["category"]}
                for m in vector_matches[:3]
            ]

        return result

    def _merge_kb_enrichments(self, regex_kb: dict, vector_kb: dict) -> dict:
        """Merge regex-based and vector-based KB enrichments."""
        if not vector_kb.get("pattern_matched") and not regex_kb.get("pattern_matched"):
            return regex_kb or {}

        # If only one matched, use that
        if vector_kb.get("pattern_matched") and not regex_kb.get("pattern_matched"):
            return vector_kb
        if regex_kb.get("pattern_matched") and not vector_kb.get("pattern_matched"):
            return regex_kb

        # Both matched: merge with vector KB solutions supplementing regex KB
        merged = dict(regex_kb)
        vector_solutions = vector_kb.get("known_solutions", [])
        existing_solutions = set(merged.get("known_solutions", []))
        for sol in vector_solutions:
            if sol not in existing_solutions:
                merged.setdefault("known_solutions", []).append(sol)

        # Use vector KB's additional data
        merged["prevention"] = vector_kb.get("prevention", merged.get("prevention", []))
        merged["similar_errors"] = vector_kb.get("similar_errors", [])
        merged["match_confidence"] = vector_kb.get("match_confidence", 0)

        return merged

    def _get_ai_analysis(self, error_details: dict, kb_enrichment: dict) -> dict:
        """Call Gemini API for AI-powered error analysis."""
        try:
            # Build the prompt with error context
            prompt = self._build_prompt(error_details, kb_enrichment)

            response = self.model.generate_content(prompt)
            response_text = response.text.strip()

            # Clean up response (remove markdown code fences if present)
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                # Remove first and last lines (``` markers)
                lines = [l for l in lines if not l.strip().startswith("```")]
                response_text = "\n".join(lines)

            ai_result = json.loads(response_text)
            logger.info("AI analysis completed successfully")
            return ai_result

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse AI response as JSON: {e}")
            return self._fallback_analysis(error_details, kb_enrichment)
        except Exception as e:
            logger.error(f"AI analysis failed: {e}")
            return self._fallback_analysis(error_details, kb_enrichment)

    def _build_prompt(self, error_details: dict, kb_enrichment: dict) -> str:
        """Build the analysis prompt with full error context."""
        # Summarize activity information
        failed_activities_summary = []
        for act in error_details.get("failed_activities", []):
            failed_activities_summary.append({
                "activity_name": act.get("activity_name"),
                "activity_type": act.get("activity_type"),
                "error": act.get("error"),
                "duration_ms": act.get("duration_ms"),
            })

        succeeded_count = len(error_details.get("succeeded_activities", []))

        context = {
            "pipeline_name": error_details.get("pipeline_name"),
            "run_id": error_details.get("run_id"),
            "primary_error_code": error_details.get("primary_error_code"),
            "primary_error_message": truncate_string(
                error_details.get("primary_error_message", ""), 2000
            ),
            "primary_failure_type": error_details.get("primary_failure_type"),
            "failing_activity_name": error_details.get("failing_activity_name"),
            "failing_activity_type": error_details.get("failing_activity_type"),
            "pipeline_parameters": error_details.get("parameters", {}),
            "invoked_by": error_details.get("invoked_by", {}),
            "run_start": str(error_details.get("run_start", "")),
            "run_end": str(error_details.get("run_end", "")),
            "duration_ms": error_details.get("duration_ms"),
            "total_activities": error_details.get("total_activities"),
            "failed_activities": failed_activities_summary,
            "succeeded_activities_count": succeeded_count,
        }

        # Add KB context if available
        if kb_enrichment.get("pattern_matched"):
            context["knowledge_base_match"] = {
                "category": kb_enrichment.get("category"),
                "known_causes": kb_enrichment.get("known_causes"),
                "severity": kb_enrichment.get("severity"),
            }

        prompt = (
            "Analyze this Azure Data Factory pipeline failure and provide "
            "a detailed diagnostic report.\n\n"
            f"Error Context:\n{json.dumps(context, indent=2, default=str)}\n\n"
            "Provide your analysis in the JSON format specified in your instructions."
        )

        return prompt

    def _merge_analysis(
        self, error_details: dict, kb_enrichment: dict, ai_analysis: dict
    ) -> dict:
        """Merge knowledge base and AI analysis into a unified report."""
        # AI analysis takes priority but is enriched with KB data
        result = {
            # Error identification
            "pipeline_name": error_details.get("pipeline_name"),
            "run_id": error_details.get("run_id"),
            "failing_activity": error_details.get("failing_activity_name"),
            "failing_activity_type": error_details.get("failing_activity_type"),

            # Error explanation
            "plain_english_error": ai_analysis.get(
                "plain_english_error",
                f"Pipeline '{error_details.get('pipeline_name')}' failed "
                f"at activity '{error_details.get('failing_activity_name')}'."
            ),
            "raw_error_message": error_details.get("primary_error_message", ""),
            "error_code": error_details.get("primary_error_code", ""),

            # Classification
            "category": ai_analysis.get(
                "category", kb_enrichment.get("category", "unknown")
            ),
            "severity": ai_analysis.get(
                "severity", kb_enrichment.get("severity", "medium")
            ),

            # Root cause
            "root_cause": ai_analysis.get("root_cause", "Unable to determine root cause"),

            # Solutions - merge AI and KB solutions
            "solutions": ai_analysis.get("solutions", []),
            "known_solutions": kb_enrichment.get("known_solutions", []),

            # Runbook
            "runbook": kb_enrichment.get("runbook"),

            # Additional context
            "preventive_measures": ai_analysis.get("preventive_measures", []),
            "additional_checks": ai_analysis.get("additional_checks", []),
            "data_engineering_tips": ai_analysis.get("data_engineering_tips", ""),
            "estimated_fix_time": kb_enrichment.get("estimated_fix_time", "unknown"),

            # Documentation
            "documentation_links": (
                ai_analysis.get("related_documentation", [])
                + [{"title": "Microsoft Docs", "url": url}
                   for url in kb_enrichment.get("documentation_links", [])]
            ),

            # Pipeline context
            "run_start": error_details.get("run_start"),
            "run_end": error_details.get("run_end"),
            "duration_ms": error_details.get("duration_ms"),
            "parameters": error_details.get("parameters", {}),
            "invoked_by": error_details.get("invoked_by", {}),
            "total_activities": error_details.get("total_activities", 0),
            "failed_activities": error_details.get("failed_activities", []),
            "succeeded_activities_count": len(
                error_details.get("succeeded_activities", [])
            ),

            # Knowledge base match info
            "kb_pattern_matched": kb_enrichment.get("pattern_matched", False),
            "kb_error_id": (
                kb_enrichment.get("error_entry", {}).get("id")
                if kb_enrichment.get("error_entry")
                else None
            ),
        }

        return result

    def _fallback_analysis(self, error_details: dict, kb_enrichment: dict = None) -> dict:
        """Fallback analysis when AI is unavailable — uses knowledge base data."""
        kb_enrichment = kb_enrichment or {}
        kb_matched = kb_enrichment.get("pattern_matched", False)
        error_entry = kb_enrichment.get("error_entry", {})

        # Build a meaningful plain-english error
        pipeline = error_details.get("pipeline_name", "Unknown")
        activity = error_details.get("failing_activity_name", "Unknown")
        error_msg = error_details.get("primary_error_message", "No details")

        if kb_matched:
            plain_error = (
                f"The pipeline '{pipeline}' failed at activity '{activity}'. "
                f"This is a known error: {error_entry.get('title', '')}. "
                f"{error_entry.get('description', '')}"
            )
            root_cause = ". ".join(kb_enrichment.get("known_causes", [])[:3]) or "See known causes above"
            category = kb_enrichment.get("category", "unknown")
            severity = kb_enrichment.get("severity", "medium")

            # Build solutions from KB
            solutions = []
            for i, sol in enumerate(kb_enrichment.get("known_solutions", [])[:5]):
                solutions.append({
                    "title": sol,
                    "steps": [],
                    "estimated_time": kb_enrichment.get("estimated_fix_time", "15-30 minutes"),
                    "likelihood": "high" if i == 0 else "medium",
                })

            # Add runbook steps as a solution
            runbook = kb_enrichment.get("runbook")
            if runbook:
                runbook_steps = runbook.get("steps", [])
                if runbook_steps:
                    solutions.insert(0, {
                        "title": f"Follow Runbook: {runbook.get('title', 'Troubleshooting Guide')}",
                        "steps": runbook_steps[:8],
                        "estimated_time": runbook.get("estimated_time", "15-30 minutes"),
                        "likelihood": "high",
                    })

            prevention = runbook.get("prevention", []) if runbook else []
        else:
            plain_error = (
                f"The pipeline '{pipeline}' failed at activity '{activity}'. "
                f"Error: {truncate_string(error_msg, 300)}"
            )
            root_cause = "Could not automatically determine root cause. Please review the error message."
            category = "unknown"
            severity = "medium"
            solutions = [{
                "title": "Manual Investigation",
                "steps": [
                    "Open Azure Data Factory Monitor",
                    f"Find the pipeline run with ID: {error_details.get('run_id', 'N/A')}",
                    "Review the failing activity error details",
                    "Check the activity input/output for data issues",
                    "Review linked service connections",
                ],
                "estimated_time": "15-30 minutes",
                "likelihood": "medium",
            }]
            prevention = []

        return {
            "plain_english_error": plain_error,
            "root_cause": root_cause,
            "category": category,
            "severity": severity,
            "solutions": solutions,
            "preventive_measures": prevention or [
                "Add monitoring alerts for pipeline failures",
                "Implement retry policies on activities",
                "Add data validation activities",
            ],
            "related_documentation": [
                {"title": d, "url": d}
                for d in kb_enrichment.get("documentation_links", [])
            ],
            "additional_checks": [],
            "data_engineering_tips": "Consider adding logging and validation steps to your pipeline.",
        }

    def quick_analyze(self, error_message: str, pipeline_name: str = "Unknown") -> dict:
        """
        Quick analysis from just an error message (no full error_details needed).
        Useful for CLI and quick lookups.
        """
        simple_details = {
            "pipeline_name": pipeline_name,
            "run_id": "N/A",
            "primary_error_message": error_message,
            "primary_error_code": "",
            "primary_failure_type": "",
            "failing_activity_name": "Unknown",
            "failing_activity_type": "Unknown",
            "parameters": {},
            "invoked_by": {},
            "run_start": None,
            "run_end": None,
            "duration_ms": None,
            "total_activities": 0,
            "failed_activities": [],
            "succeeded_activities": [],
        }
        return self.analyze(simple_details)
