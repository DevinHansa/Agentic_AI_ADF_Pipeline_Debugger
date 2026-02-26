"""
ADF Pipeline Debugger - Knowledge Base
Pre-built database of common ADF errors with pattern matching and resolution runbooks.
"""
import re
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("adf_debugger.knowledge_base")


class KnowledgeBase:
    """
    Knowledge base for common ADF errors and resolution runbooks.
    Uses regex pattern matching to identify known errors before calling AI.
    """

    def __init__(self, knowledge_dir: Path = None):
        if knowledge_dir is None:
            knowledge_dir = Path(__file__).parent.parent / "knowledge"

        self.knowledge_dir = knowledge_dir
        self.errors = []
        self.runbooks = {}
        self._load_data()

    def _load_data(self):
        """Load error patterns and runbooks from JSON files."""
        # Load common errors
        errors_file = self.knowledge_dir / "common_errors.json"
        if errors_file.exists():
            with open(errors_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.errors = data.get("errors", [])
            logger.info(f"Loaded {len(self.errors)} error patterns")
        else:
            logger.warning(f"Error patterns file not found: {errors_file}")

        # Load runbooks
        runbooks_file = self.knowledge_dir / "runbooks.json"
        if runbooks_file.exists():
            with open(runbooks_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.runbooks = data.get("runbooks", {})
            logger.info(f"Loaded {len(self.runbooks)} runbooks")
        else:
            logger.warning(f"Runbooks file not found: {runbooks_file}")

    def match_error(self, error_message: str) -> Optional[dict]:
        """
        Match an error message against known patterns.
        Returns the best matching error entry or None.
        """
        if not error_message:
            return None

        best_match = None
        best_score = 0

        for error in self.errors:
            pattern = error.get("pattern", "")
            try:
                matches = re.findall(pattern, error_message, re.IGNORECASE)
                if matches:
                    # Score by number of pattern matches (more = better fit)
                    score = len(matches)
                    if score > best_score:
                        best_score = score
                        best_match = error
            except re.error:
                logger.warning(f"Invalid regex pattern: {pattern}")
                continue

        if best_match:
            logger.info(
                f"Matched error pattern: {best_match['id']} - {best_match['title']}"
            )
        else:
            logger.info("No matching error pattern found")

        return best_match

    def get_runbook(self, category: str) -> Optional[dict]:
        """Get the resolution runbook for an error category."""
        runbook = self.runbooks.get(category)
        if runbook:
            logger.info(f"Found runbook for category: {category}")
        return runbook

    def get_all_categories(self) -> list:
        """Get all available error categories."""
        return list(set(e.get("category", "unknown") for e in self.errors))

    def search_errors(self, query: str) -> list:
        """Search error database by keyword."""
        results = []
        query_lower = query.lower()
        for error in self.errors:
            searchable = (
                f"{error.get('title', '')} {error.get('description', '')} "
                f"{' '.join(error.get('common_causes', []))}"
            ).lower()
            if query_lower in searchable:
                results.append(error)
        return results

    def get_error_by_id(self, error_id: str) -> Optional[dict]:
        """Get a specific error entry by its ID."""
        for error in self.errors:
            if error.get("id") == error_id:
                return error
        return None

    def get_enrichment(self, error_message: str) -> dict:
        """
        Get full enrichment for an error message: pattern match + runbook.
        This is the main entry point for knowledge base lookups.
        """
        matched = self.match_error(error_message)

        enrichment = {
            "pattern_matched": matched is not None,
            "error_entry": matched,
            "runbook": None,
            "category": "unknown",
            "known_causes": [],
            "known_solutions": [],
            "severity": "medium",
            "estimated_fix_time": "unknown",
            "documentation_links": [],
        }

        if matched:
            category = matched.get("category", "unknown")
            enrichment["category"] = category
            enrichment["known_causes"] = matched.get("common_causes", [])
            enrichment["known_solutions"] = matched.get("solutions", [])
            enrichment["severity"] = matched.get("severity", "medium")
            enrichment["estimated_fix_time"] = matched.get("estimated_fix_time", "unknown")
            enrichment["documentation_links"] = matched.get("docs", [])
            enrichment["runbook"] = self.get_runbook(category)

        return enrichment
