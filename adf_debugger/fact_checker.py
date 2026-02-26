"""
ADF Pipeline Debugger - AI Fact-Checking Agent
Verifies analysis accuracy before sending emails.
Cross-references AI analysis against the vector knowledge base.
"""
import json
import logging
from typing import Optional

import google.generativeai as genai

logger = logging.getLogger("adf_debugger.fact_checker")

FACT_CHECK_PROMPT = """You are a senior Azure Data Factory expert performing fact-checking on an AI-generated error analysis report.

Your job is to verify the accuracy of the analysis and assign a confidence score.

Given:
1. The ORIGINAL ERROR message from ADF
2. The AI-GENERATED ANALYSIS
3. KNOWLEDGE BASE matches (verified patterns from Azure documentation)

Evaluate:
1. Is the root cause assessment accurate for this error?
2. Are the suggested solutions actually applicable to this specific error?
3. Is the severity assessment correct?
4. Are the preventive measures practical and relevant?
5. Overall confidence in the analysis (0.0 to 1.0)

Respond in JSON format:
{
    "confidence_score": 0.85,
    "confidence_level": "high|medium|low",
    "root_cause_accurate": true,
    "solutions_applicable": true,
    "severity_correct": true,
    "corrections": [],
    "additional_insights": "",
    "verified_solutions": ["solution1", "solution2"],
    "flagged_issues": []
}
"""


class FactCheckingAgent:
    """
    AI-powered fact-checking agent that verifies error analysis accuracy.
    Uses Gemini AI to cross-reference analysis against known patterns.
    """

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            model_name=model,
            system_instruction=FACT_CHECK_PROMPT,
        )
        self.min_confidence = 0.5  # Minimum confidence to send email
        logger.info("Fact-checking agent initialized")

    def verify(self, error_details: dict, analysis: dict, kb_matches: list = None) -> dict:
        """
        Verify the accuracy of an error analysis.
        
        Args:
            error_details: Raw error details from ADF
            analysis: AI-generated analysis to verify
            kb_matches: Knowledge base matches for cross-reference
            
        Returns:
            Verification result with confidence score
        """
        try:
            prompt = self._build_verification_prompt(error_details, analysis, kb_matches)
            response = self.model.generate_content(prompt)
            response_text = response.text.strip()

            # Clean markdown fences
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                lines = [l for l in lines if not l.strip().startswith("```")]
                response_text = "\n".join(lines)

            result = json.loads(response_text)
            
            # Ensure required fields
            result.setdefault("confidence_score", 0.5)
            result.setdefault("confidence_level", self._score_to_level(result["confidence_score"]))
            result.setdefault("root_cause_accurate", True)
            result.setdefault("solutions_applicable", True)
            result.setdefault("severity_correct", True)
            result.setdefault("corrections", [])
            result.setdefault("flagged_issues", [])
            
            logger.info(f"Fact-check complete: confidence={result['confidence_score']:.2f} ({result['confidence_level']})")
            return result

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse fact-check response: {e}")
            return self._fallback_verification(analysis, kb_matches)
        except Exception as e:
            logger.error(f"Fact-checking failed: {e}")
            return self._fallback_verification(analysis, kb_matches)

    def _build_verification_prompt(self, error_details: dict, analysis: dict, kb_matches: list) -> str:
        """Build the fact-checking prompt."""
        context = {
            "original_error": {
                "pipeline": error_details.get("pipeline_name"),
                "activity": error_details.get("failing_activity_name"),
                "error_message": error_details.get("primary_error_message", "")[:1000],
                "error_code": error_details.get("primary_error_code", ""),
                "failure_type": error_details.get("primary_failure_type", ""),
            },
            "ai_analysis": {
                "root_cause": analysis.get("root_cause", ""),
                "category": analysis.get("category", ""),
                "severity": analysis.get("severity", ""),
                "solutions": [s.get("title", "") for s in analysis.get("solutions", [])],
                "preventive_measures": analysis.get("preventive_measures", []),
            },
            "kb_matches": [
                {
                    "title": m.get("entry", {}).get("title", ""),
                    "category": m.get("entry", {}).get("category", ""),
                    "similarity": m.get("similarity", 0),
                }
                for m in (kb_matches or [])[:3]
            ],
        }

        return (
            "Fact-check this ADF pipeline failure analysis:\n\n"
            f"{json.dumps(context, indent=2, default=str)}\n\n"
            "Respond with your verification in JSON format."
        )

    def _fallback_verification(self, analysis: dict, kb_matches: list = None) -> dict:
        """Fallback verification when AI is unavailable — uses KB match quality."""
        confidence = 0.5  # Default confidence

        # Boost confidence if KB matches found
        if kb_matches:
            best_sim = kb_matches[0].get("similarity", 0) if kb_matches else 0
            if best_sim > 0.6:
                confidence = 0.85
            elif best_sim > 0.4:
                confidence = 0.70
            elif best_sim > 0.3:
                confidence = 0.60

        # Check if analysis has content
        if analysis.get("root_cause") and analysis.get("solutions"):
            confidence = min(confidence + 0.1, 0.95)

        # Check if KB confirms the category
        if kb_matches and analysis.get("category"):
            kb_cat = kb_matches[0].get("entry", {}).get("category", "")
            if kb_cat == analysis.get("category"):
                confidence = min(confidence + 0.1, 0.95)

        return {
            "confidence_score": round(confidence, 2),
            "confidence_level": self._score_to_level(confidence),
            "root_cause_accurate": confidence > 0.6,
            "solutions_applicable": confidence > 0.5,
            "severity_correct": True,
            "corrections": [],
            "flagged_issues": [] if confidence > 0.5 else ["Low confidence — manual review recommended"],
            "verification_method": "knowledge_base_fallback",
        }

    def should_send_email(self, verification: dict) -> bool:
        """Determine if the analysis is confident enough to send email."""
        return verification.get("confidence_score", 0) >= self.min_confidence

    @staticmethod
    def _score_to_level(score: float) -> str:
        """Convert numeric score to confidence level."""
        if score >= 0.8:
            return "high"
        elif score >= 0.6:
            return "medium"
        else:
            return "low"
