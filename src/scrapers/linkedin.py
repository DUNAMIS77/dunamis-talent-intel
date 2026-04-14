"""
LinkedIn scraper via Proxycurl API.

Proxycurl is a paid API that provides structured LinkedIn data legally
(scrapes on your behalf, handles ToS). Each call costs credits.

Key endpoints used:
  GET /proxycurl/api/v2/linkedin           — single profile lookup by URL
  GET /proxycurl/api/linkedin/company/employees/  — all employees at a firm
  GET /proxycurl/api/search/person/        — search by role/company/keyword

Cost awareness:
  - Profile lookup: ~1 credit
  - Company employees: ~1 credit per employee returned
  - Person search: ~3 credits per call (returns up to 10 results)
  - This module tracks credits spent and warns when budget is exceeded.

Strategy:
  Phase A — Company sweep: for each target firm, pull all investment staff
  Phase B — Person search: search by role keywords across all Korean finance
  Phase C — Profile enrich: fetch full profiles for candidates already in DB
             that have a linkedin_url but no enrichment
"""

import httpx
import time
import logging
from datetime import datetime
from typing import Iterator

from src.config import PROXYCURL_API_KEY
from src.database.models import Candidate
from src.database.db import get_session

logger = logging.getLogger(__name__)

_BASE_URL = "https://nubela.co/proxycurl/api"

HEADERS = {
    "Authorization": f"Bearer {PROXYCURL_API_KEY}",
    "Accept": "application/json",
}

# Budget guard — stop if credits spent exceed this in a single run
DEFAULT_CREDIT_BUDGET = 500

# ── Target firms for company sweep ────────────────────────────────────────────

# LinkedIn company slugs / URLs for major Korean finance firms
# Format: (display_name, linkedin_company_url)
TARGET_FIRMS: list[tuple[str, str]] = [
    ("삼성자산운용", "https://www.linkedin.com/company/samsung-asset-management/"),
    ("미래에셋자산운용", "https://www.linkedin.com/company/mirae-asset-global-investments/"),
    ("KB자산운용", "https://www.linkedin.com/company/kb-asset-management/"),
    ("신한자산운용", "https://www.linkedin.com/company/shinhan-asset-management/"),
    ("한국투자신탁운용", "https://www.linkedin.com/company/korea-investment-management/"),
    ("타임폴리오자산운용", "https://www.linkedin.com/company/timefolio/"),
    ("트러스톤자산운용", "https://www.linkedin.com/company/truston-asset-management/"),
    ("VIP자산운용", "https://www.linkedin.com/company/vip-asset-management/"),
    ("브레인자산운용", "https://www.linkedin.com/company/brain-asset-management/"),
    ("쿼드자산운용", "https://www.linkedin.com/company/quad-investment-management/"),
    ("라임자산운용", "https://www.linkedin.com/company/lime-asset-management/"),
    ("삼성증권", "https://www.linkedin.com/company/samsung-securities/"),
    ("미래에셋증권", "https://www.linkedin.com/company/mirae-asset-securities/"),
    ("KB증권", "https://www.linkedin.com/company/kb-securities/"),
    ("NH투자증권", "https://www.linkedin.com/company/nh-investment-securities/"),
    ("한국투자증권", "https://www.linkedin.com/company/korea-investment-securities/"),
    ("키움증권", "https://www.linkedin.com/company/kiwoom-securities/"),
    ("메리츠증권", "https://www.linkedin.com/company/meritz-securities/"),
]

# Role keywords to filter employees — only fetch investment professionals
INVESTMENT_ROLE_KEYWORDS = [
    "analyst", "fund manager", "portfolio", "investment", "equity", "research",
    "애널리스트", "펀드매니저", "운용", "투자", "리서치", "포트폴리오",
    "CIO", "chief investment", "sector",
]

# Person search role filters
SEARCH_ROLES = [
    "equity analyst", "fund manager", "portfolio manager",
    "investment analyst", "buy-side analyst", "sell-side analyst",
    "hedge fund manager", "research analyst",
]


# ── API helpers ───────────────────────────────────────────────────────────────

class CreditBudgetExceeded(Exception):
    pass


class ProxycurlClient:
    """Thin wrapper around the Proxycurl API with credit tracking."""

    def __init__(self, api_key: str, budget: int = DEFAULT_CREDIT_BUDGET):
        self.api_key = api_key
        self.budget = budget
        self.credits_spent = 0
        self._headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}

    def _get(self, path: str, params: dict) -> dict | None:
        if self.credits_spent >= self.budget:
            raise CreditBudgetExceeded(
                f"Credit budget of {self.budget} reached (spent: {self.credits_spent})"
            )
        url = f"{_BASE_URL}{path}"
        try:
            with httpx.Client(headers=self._headers, timeout=20) as client:
                resp = client.get(url, params=params)
                # Track credits from response header
                if "X-Proxycurl-Credit-Cost" in resp.headers:
                    self.credits_spent += int(resp.headers["X-Proxycurl-Credit-Cost"])
                resp.raise_for_status()
                return resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            if exc.response.status_code == 429:
                logger.warning("Proxycurl rate limited — sleeping 60s")
                time.sleep(60)
                return None
            logger.warning("Proxycurl HTTP error %s: %s", exc.response.status_code, exc)
            return None
        except httpx.HTTPError as exc:
            logger.warning("Proxycurl network error: %s", exc)
            return None

    def get_profile(self, linkedin_url: str) -> dict | None:
        """Fetch a single LinkedIn profile by URL."""
        return self._get("/v2/linkedin", {
            "url": linkedin_url,
            "use_cache": "if-present",
            "fallback_to_cache": "on-error",
        })

    def get_company_employees(
        self,
        company_url: str,
        role_keyword: str | None = None,
        page_token: str | None = None,
    ) -> dict | None:
        """Fetch employees at a company, optionally filtered by role keyword."""
        params: dict = {
            "url": company_url,
            "use_cache": "if-present",
            "enrich_profiles": "enrich",  # return full profiles, not just URLs
        }
        if role_keyword:
            params["keyword_regex"] = role_keyword
        if page_token:
            params["page_token"] = page_token
        return self._get("/linkedin/company/employees/", params)

    def search_people(
        self,
        role: str,
        country: str = "KR",
        current_company_type: str | None = None,
        page_token: str | None = None,
    ) -> dict | None:
        """Search for people by role and country."""
        params: dict = {
            "country": country,
            "current_role_title": role,
            "enrich_profiles": "enrich",
        }
        if current_company_type:
            params["current_company_type"] = current_company_type
        if page_token:
            params["page_token"] = page_token
        return self._get("/search/person/", params)

    def credit_report(self) -> str:
        return f"Credits spent: {self.credits_spent} / {self.budget}"


# ── Profile parsing ───────────────────────────────────────────────────────────

def _is_investment_role(title: str) -> bool:
    """Return True if a role title looks like an investment professional."""
    if not title:
        return False
    title_lower = title.lower()
    return any(kw.lower() in title_lower for kw in INVESTMENT_ROLE_KEYWORDS)


def _parse_sector_from_profile(profile: dict) -> list[str]:
    """
    Infer sector coverage from LinkedIn profile headline, summary, and experience.
    Simple keyword matching — Claude enrichment does the deeper analysis.
    """
    SECTOR_KEYWORDS = {
        "TMT": ["tech", "technology", "telecom", "media", "it", "소프트웨어", "IT", "반도체", "통신"],
        "Healthcare": ["healthcare", "pharma", "biotech", "bio", "바이오", "헬스케어", "제약"],
        "Consumer": ["consumer", "retail", "소비재", "유통", "패션"],
        "Financials": ["financial", "bank", "insurance", "금융", "보험", "은행"],
        "Energy/Materials": ["energy", "oil", "chemical", "steel", "에너지", "화학", "철강"],
        "Industrials": ["industrial", "manufacturing", "auto", "제조", "자동차", "기계"],
        "Real Estate": ["real estate", "reit", "부동산"],
    }
    text = " ".join([
        profile.get("headline", "") or "",
        profile.get("summary", "") or "",
        " ".join(
            (exp.get("title", "") or "") + " " + (exp.get("description", "") or "")
            for exp in (profile.get("experiences") or [])
        ),
    ]).lower()

    found = []
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(kw.lower() in text for kw in keywords):
            found.append(sector)
    return found or ["Unknown"]


def _parse_career_history(profile: dict) -> list[dict]:
    """Extract career history from LinkedIn experiences list."""
    history = []
    for exp in (profile.get("experiences") or []):
        entry: dict = {}
        if company := (exp.get("company") or exp.get("company_linkedin_profile_url", "")):
            entry["firm"] = company if isinstance(company, str) else exp.get("company", "")
        if title := exp.get("title"):
            entry["role"] = title
        starts_at = exp.get("starts_at") or {}
        ends_at = exp.get("ends_at") or {}
        if starts_at.get("year"):
            end_year = ends_at.get("year", "present")
            entry["years"] = f"{starts_at['year']}–{end_year}"
        if entry.get("firm") or entry.get("role"):
            history.append(entry)
    return history


def _parse_certifications(profile: dict) -> list[str]:
    certs = []
    for cert in (profile.get("accomplishment_certifications") or []):
        name = cert.get("name", "")
        if name:
            certs.append(name)
    return certs


def _infer_seniority(title: str, exp_years: int) -> str:
    if not title:
        title = ""
    title_lower = title.lower()
    if any(k in title_lower for k in ["cio", "chief", "managing director", "partner", "대표", "전무"]):
        return "partner"
    if any(k in title_lower for k in ["director", "md", "head", "이사", "본부장"]):
        return "director"
    if any(k in title_lower for k in ["vp", "vice president", "팀장", "수석", "부장"]):
        return "vp"
    if exp_years >= 8:
        return "vp"
    if any(k in title_lower for k in ["associate", "과장", "차장"]):
        return "associate"
    if exp_years >= 4:
        return "associate"
    return "analyst"


def profile_to_candidate(profile: dict, source_firm: str | None = None) -> Candidate | None:
    """Convert a raw Proxycurl profile dict to a Candidate."""
    name = (profile.get("full_name") or "").strip()
    if not name:
        return None

    current_exp = next(
        (e for e in (profile.get("experiences") or []) if not e.get("ends_at")),
        None,
    )
    current_firm = (
        (current_exp or {}).get("company", "")
        or source_firm
        or ""
    )
    current_role = (current_exp or {}).get("title", "") or profile.get("headline", "")

    # Count total years of experience
    exp_years = 0
    for exp in (profile.get("experiences") or []):
        starts = (exp.get("starts_at") or {}).get("year")
        ends = (exp.get("ends_at") or {}).get("year") or datetime.utcnow().year
        if starts:
            exp_years += max(0, ends - starts)

    linkedin_url = profile.get("public_identifier")
    if linkedin_url and not linkedin_url.startswith("http"):
        linkedin_url = f"https://www.linkedin.com/in/{linkedin_url}"

    firm_type = _infer_firm_type_from_profile(current_firm, profile)

    return Candidate(
        name=name,
        current_firm=current_firm,
        firm_type=firm_type,
        role=current_role,
        seniority=_infer_seniority(current_role, exp_years),
        sector_coverage=_parse_sector_from_profile(profile),
        career_history=_parse_career_history(profile),
        certifications=_parse_certifications(profile),
        linkedin_url=linkedin_url,
        source="linkedin",
        source_url=linkedin_url or "",
        sourced_at=datetime.utcnow(),
        raw_data={
            "headline": profile.get("headline"),
            "summary": profile.get("summary"),
            "education": profile.get("education"),
            "experience_years": exp_years,
        },
    )


def _infer_firm_type_from_profile(firm: str, profile: dict) -> str:
    firm_lower = (firm or "").lower()
    if any(k in firm_lower for k in ["asset", "운용", "자산운용", "fund"]):
        return "asset_manager"
    if any(k in firm_lower for k in ["securities", "증권", "brokerage"]):
        return "broker"
    if any(k in firm_lower for k in ["hedge", "헤지", "partners", "파트너"]):
        return "hedge_fund"
    if any(k in firm_lower for k in ["bank", "은행", "capital"]):
        return "bank"
    return "other"


# ── DB helpers ────────────────────────────────────────────────────────────────

def upsert_candidate(session, candidate: Candidate) -> tuple[Candidate, bool]:
    """Insert or update. Dedup on name + current_firm; LinkedIn URL as fallback."""
    # Try by LinkedIn URL first
    if candidate.linkedin_url:
        existing = (
            session.query(Candidate)
            .filter_by(linkedin_url=candidate.linkedin_url)
            .first()
        )
        if existing:
            existing.enriched_at = candidate.enriched_at
            existing.career_history = candidate.career_history or existing.career_history
            existing.sector_coverage = candidate.sector_coverage or existing.sector_coverage
            existing.certifications = candidate.certifications or existing.certifications
            return existing, False

    # Fall back to name + firm
    existing = (
        session.query(Candidate)
        .filter_by(name=candidate.name, current_firm=candidate.current_firm)
        .first()
    )
    if existing:
        if not existing.linkedin_url and candidate.linkedin_url:
            existing.linkedin_url = candidate.linkedin_url
        existing.career_history = candidate.career_history or existing.career_history
        existing.sector_coverage = candidate.sector_coverage or existing.sector_coverage
        return existing, False

    session.add(candidate)
    return candidate, True


# ── Main scrapers ─────────────────────────────────────────────────────────────

def run_company_sweep(
    credit_budget: int = DEFAULT_CREDIT_BUDGET,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Phase A: Sweep all employees at TARGET_FIRMS, keep investment roles only.
    Most credit-intensive — budget carefully.
    """
    if not PROXYCURL_API_KEY:
        logger.error("PROXYCURL_API_KEY not set.")
        return 0, 0

    from src.database.db import init_db
    init_db()

    client = ProxycurlClient(PROXYCURL_API_KEY, budget=credit_budget)
    session = get_session()
    created, updated = 0, 0

    try:
        for firm_name, firm_url in TARGET_FIRMS:
            logger.info("Sweeping employees: %s", firm_name)
            page_token = None

            while True:
                try:
                    data = client.get_company_employees(
                        company_url=firm_url,
                        role_keyword="|".join(["analyst", "manager", "investment", "portfolio"]),
                        page_token=page_token,
                    )
                except CreditBudgetExceeded as exc:
                    logger.warning("Budget exceeded: %s", exc)
                    break

                if not data:
                    break

                employees = data.get("employees", [])
                for emp in employees:
                    profile = emp.get("profile") or emp
                    if not _is_investment_role(profile.get("headline", "") or ""):
                        # Also check current role
                        exps = profile.get("experiences") or []
                        current = next((e for e in exps if not e.get("ends_at")), None)
                        if not current or not _is_investment_role(current.get("title", "")):
                            continue

                    candidate = profile_to_candidate(profile, source_firm=firm_name)
                    if not candidate:
                        continue

                    if dry_run:
                        print(f"  {candidate.name} | {candidate.current_firm} | {candidate.role}")
                        continue

                    _, is_new = upsert_candidate(session, candidate)
                    if is_new:
                        created += 1
                    else:
                        updated += 1

                    if (created + updated) % 50 == 0:
                        session.commit()

                page_token = data.get("next_page")
                if not page_token:
                    break
                time.sleep(1.0)

            time.sleep(2.0)

        session.commit()
        logger.info(
            "Company sweep done — created: %d, updated: %d | %s",
            created, updated, client.credit_report(),
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return created, updated


def run_person_search(
    credit_budget: int = 200,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Phase B: Search for Korean finance professionals by role keyword.
    Lower credit cost than company sweep.
    """
    if not PROXYCURL_API_KEY:
        logger.error("PROXYCURL_API_KEY not set.")
        return 0, 0

    from src.database.db import init_db
    init_db()

    client = ProxycurlClient(PROXYCURL_API_KEY, budget=credit_budget)
    session = get_session()
    created, updated = 0, 0

    try:
        for role in SEARCH_ROLES:
            logger.info("Searching: %s in KR", role)
            page_token = None
            while True:
                try:
                    data = client.search_people(role=role, country="KR", page_token=page_token)
                except CreditBudgetExceeded as exc:
                    logger.warning("Budget exceeded: %s", exc)
                    break

                if not data:
                    break

                results = data.get("results", [])
                for item in results:
                    profile = item.get("linkedin_profile_url") and item or item
                    # If enrich_profiles=enrich, profile data is embedded
                    if isinstance(profile.get("profile"), dict):
                        profile = profile["profile"]

                    candidate = profile_to_candidate(profile)
                    if not candidate:
                        continue

                    if dry_run:
                        print(f"  {candidate.name} | {candidate.current_firm} | {candidate.role}")
                        continue

                    _, is_new = upsert_candidate(session, candidate)
                    if is_new:
                        created += 1
                    else:
                        updated += 1

                    if (created + updated) % 50 == 0:
                        session.commit()

                page_token = data.get("next_page")
                if not page_token:
                    break
                time.sleep(1.5)

            time.sleep(2.0)

        session.commit()
        logger.info(
            "Person search done — created: %d, updated: %d | %s",
            created, updated, client.credit_report(),
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return created, updated


def run_profile_enrich(
    credit_budget: int = 100,
    dry_run: bool = False,
) -> int:
    """
    Phase C: Fetch full profiles for candidates already in DB who have
    a linkedin_url but were sourced from KOFIA/Wanted (no full profile yet).
    """
    if not PROXYCURL_API_KEY:
        logger.error("PROXYCURL_API_KEY not set.")
        return 0

    from src.database.db import init_db
    init_db()

    client = ProxycurlClient(PROXYCURL_API_KEY, budget=credit_budget)
    session = get_session()
    enriched = 0

    try:
        # Candidates with LinkedIn URL but sourced from non-LinkedIn source
        candidates = (
            session.query(Candidate)
            .filter(
                Candidate.linkedin_url.isnot(None),
                Candidate.source != "linkedin",
                Candidate.career_history.is_(None),
            )
            .limit(credit_budget)
            .all()
        )

        logger.info("Profile enrich: %d candidates to process", len(candidates))

        for candidate in candidates:
            try:
                profile = client.get_profile(candidate.linkedin_url)
            except CreditBudgetExceeded:
                break

            if not profile:
                time.sleep(1.0)
                continue

            # Update fields from full profile
            candidate.career_history = _parse_career_history(profile)
            candidate.sector_coverage = _parse_sector_from_profile(profile)
            candidate.certifications = _parse_certifications(profile) or candidate.certifications
            candidate.enriched_at = datetime.utcnow()

            if not dry_run:
                raw = candidate.raw_data or {}
                raw["linkedin_full"] = {
                    "headline": profile.get("headline"),
                    "summary": profile.get("summary"),
                }
                candidate.raw_data = raw
                enriched += 1

            time.sleep(1.0)

        session.commit()
        logger.info(
            "Profile enrich done — enriched: %d | %s",
            enriched, client.credit_report(),
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return enriched
