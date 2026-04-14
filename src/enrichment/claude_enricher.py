"""
Claude API enrichment layer.

Takes raw candidate records and uses Claude to:
  1. Parse unstructured profile text into structured fields
  2. Infer sector coverage from career history
  3. Score fit for Dunamis (long/short equity, fundamental analysis)

Cost optimisations applied:
  - Prompt caching on the stable system prompt (saves ~90% on repeated calls)
  - Batch API for large enrichment runs (50% cost reduction)
  - claude-opus-4-6 with adaptive thinking for nuanced inference
"""

import json
import logging
from datetime import datetime
from typing import Any

import anthropic

from src.config import ANTHROPIC_API_KEY
from src.database.models import Candidate
from src.database.db import get_session

logger = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ── System prompt (stable — will be cached after first call) ─────────────────

ENRICHMENT_SYSTEM_PROMPT = """You are a talent intelligence analyst for Dunamis Asset Management, a Korean hedge fund running a long/short equity strategy with a fundamental bottom-up approach.

Your job is to analyse candidate profiles from Korean financial firms and extract structured intelligence.

For each candidate, output a JSON object with these fields:
{
  "sector_coverage": ["TMT", "Healthcare", "Energy", ...],   // sectors they cover/covered
  "seniority": "analyst|associate|vp|director|md|partner",
  "fit_score": 0-10,                                          // fit for L/S equity fundamental
  "fit_rationale": "1-2 sentence explanation",
  "inferred_skills": ["bottom-up analysis", "financial modelling", ...],
  "career_summary": "2-3 sentence summary of career arc",
  "approach_priority": "high|medium|low"
}

Scoring guide for fit_score:
- 8-10: Buy-side analyst/PM with L/S or fundamental experience, strong sector depth
- 6-7: Sell-side analyst with strong sector coverage, or buy-side with adjacent strategy
- 4-5: Asset management background but less directly relevant (quant, macro, passive)
- 1-3: Operations, compliance, banking — not investment professionals
- 0: Cannot determine

Sector categories to use: TMT, Healthcare, Consumer, Financials, Energy/Materials, Industrials, Real Estate, Macro, Multi-sector, Unknown

Always respond with valid JSON only — no explanation text outside the JSON.
"""


def enrich_candidate(candidate: Candidate) -> dict[str, Any] | None:
    """
    Call Claude to enrich a single candidate.
    Uses prompt caching on the system prompt.
    Returns parsed enrichment dict or None on failure.
    """
    profile_text = _build_profile_text(candidate)

    try:
        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1024,
            thinking={"type": "adaptive"},
            # Cache the system prompt — stable across all enrichment calls
            system=[{
                "type": "text",
                "text": ENRICHMENT_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{
                "role": "user",
                "content": f"Analyse this candidate profile:\n\n{profile_text}",
            }],
        )
    except anthropic.APIError as exc:
        logger.error("Claude API error enriching candidate %s: %s", candidate.id, exc)
        return None

    # Extract text block (thinking blocks precede text)
    text = next(
        (block.text for block in response.content if block.type == "text"),
        None,
    )
    if not text:
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        logger.warning("Failed to parse Claude JSON for candidate %s", candidate.id)
        return None


def _build_profile_text(candidate: Candidate) -> str:
    """Build a plain-text profile for Claude to parse."""
    parts = [
        f"Name: {candidate.name}",
        f"Current firm: {candidate.current_firm} ({candidate.firm_type or 'unknown type'})",
        f"Role: {candidate.role or 'unknown'}",
    ]
    if candidate.certifications:
        parts.append(f"Certifications: {', '.join(candidate.certifications)}")
    if candidate.career_history:
        history_lines = []
        for entry in candidate.career_history:
            line = f"  - {entry.get('firm', '?')} | {entry.get('role', '?')}"
            if entry.get("years"):
                line += f" ({entry['years']})"
            history_lines.append(line)
        parts.append("Career history:\n" + "\n".join(history_lines))
    if candidate.education:
        parts.append(f"Education: {json.dumps(candidate.education, ensure_ascii=False)}")
    if candidate.notes:
        parts.append(f"Notes: {candidate.notes}")
    return "\n".join(parts)


def apply_enrichment(candidate: Candidate, data: dict[str, Any]):
    """Write enrichment results back onto the candidate object."""
    if sectors := data.get("sector_coverage"):
        candidate.sector_coverage = sectors
    if seniority := data.get("seniority"):
        candidate.seniority = seniority
    if summary := data.get("career_summary"):
        candidate.notes = (candidate.notes or "") + f"\n[AI] {summary}"
    candidate.enriched_at = datetime.utcnow()
    # Store full enrichment output in raw_data
    raw = candidate.raw_data or {}
    raw["enrichment"] = data
    candidate.raw_data = raw


def run_enrichment(batch_size: int = 50, unenriched_only: bool = True):
    """
    Enrich candidates that haven't been enriched yet.
    Processes in batches; logs cache hit rates.
    """
    session = get_session()
    try:
        query = session.query(Candidate)
        if unenriched_only:
            query = query.filter(Candidate.enriched_at.is_(None))
        candidates = query.limit(batch_size).all()

        if not candidates:
            logger.info("No candidates to enrich.")
            return

        logger.info("Enriching %d candidates...", len(candidates))
        enriched = 0

        for candidate in candidates:
            result = enrich_candidate(candidate)
            if result:
                apply_enrichment(candidate, result)
                enriched += 1

        session.commit()
        logger.info("Enrichment complete — %d/%d succeeded", enriched, len(candidates))
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def run_batch_enrichment(candidate_ids: list[int] | None = None):
    """
    Use the Batches API for large-scale enrichment (50% cost reduction).
    Submits all requests asynchronously, polls until done.
    """
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request
    import time
    import uuid

    session = get_session()
    try:
        query = session.query(Candidate).filter(Candidate.enriched_at.is_(None))
        if candidate_ids:
            query = query.filter(Candidate.id.in_(candidate_ids))
        candidates = query.all()

        if not candidates:
            logger.info("No candidates for batch enrichment.")
            return

        logger.info("Submitting batch enrichment for %d candidates", len(candidates))

        requests = [
            Request(
                custom_id=str(candidate.id),
                params=MessageCreateParamsNonStreaming(
                    model="claude-opus-4-6",
                    max_tokens=1024,
                    system=[{
                        "type": "text",
                        "text": ENRICHMENT_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{
                        "role": "user",
                        "content": (
                            f"Analyse this candidate profile:\n\n"
                            f"{_build_profile_text(candidate)}"
                        ),
                    }],
                ),
            )
            for candidate in candidates
        ]

        batch = client.messages.batches.create(requests=requests)
        logger.info("Batch submitted: %s", batch.id)

        # Poll until done
        while True:
            batch = client.messages.batches.retrieve(batch.id)
            if batch.processing_status == "ended":
                break
            logger.info(
                "Batch %s: %d processing, %d succeeded so far",
                batch.id,
                batch.request_counts.processing,
                batch.request_counts.succeeded,
            )
            time.sleep(30)

        # Apply results
        id_map = {str(c.id): c for c in candidates}
        enriched = 0
        for result in client.messages.batches.results(batch.id):
            if result.result.type != "succeeded":
                continue
            candidate = id_map.get(result.custom_id)
            if not candidate:
                continue
            text = next(
                (b.text for b in result.result.message.content if b.type == "text"),
                None,
            )
            if not text:
                continue
            try:
                data = json.loads(text)
                apply_enrichment(candidate, data)
                enriched += 1
            except json.JSONDecodeError:
                pass

        session.commit()
        logger.info("Batch enrichment complete — %d enriched", enriched)
        return batch.id

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
