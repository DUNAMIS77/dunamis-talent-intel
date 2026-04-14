"""
Wanted (원티드) & Jumpit (점핏) scrapers.

Wanted approach:
  - Search job postings in finance/investment categories to identify
    firms actively hiring and the roles/profiles they seek.
  - Scrape the "talent" (인재풀) section where professionals publicly
    list themselves as open to opportunities.
  - Wanted exposes a JSON API used by its own frontend — we target that.

Jumpit approach:
  - Similar job-board scrape for finance sector.
  - jumpit.co.kr also has profile search for visible candidates.

Both sources yield two types of intelligence:
  1. Firm intel — who is hiring, what roles, what requirements
  2. Candidate intel — professionals who are actively looking
"""

import httpx
import time
import logging
from datetime import datetime
from typing import Iterator

from src.database.models import Candidate
from src.database.db import get_session

logger = logging.getLogger(__name__)

# ── Wanted API ────────────────────────────────────────────────────────────────

_WANTED_JOBS_URL = "https://www.wanted.co.kr/api/v4/jobs"
_WANTED_TALENT_URL = "https://www.wanted.co.kr/api/v4/talent-pool/profiles"

WANTED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://www.wanted.co.kr/",
    "Origin": "https://www.wanted.co.kr",
    "wanted_user_country": "KR",
    "wanted_user_language": "ko",
}

# Wanted category/tag IDs for finance/investment roles
# These map to 금융/투자 job categories on the platform
FINANCE_TAGS = [
    "867",   # 금융 (Finance)
    "872",   # 투자 (Investment)
    "868",   # 증권/펀드 (Securities/Funds)
    "870",   # 자산운용 (Asset Management)
    "873",   # 투자분석 (Investment Analysis)
    "874",   # 리서치 (Research)
    "871",   # 헤지펀드 (Hedge Fund)
]

# Wanted job search keywords
FINANCE_KEYWORDS = [
    "애널리스트", "펀드매니저", "포트폴리오매니저", "투자운용",
    "리서치", "주식운용", "헤지펀드", "자산운용", "투자분석",
    "equity analyst", "fund manager", "portfolio manager",
]

# ── Jumpit API ────────────────────────────────────────────────────────────────

_JUMPIT_JOBS_URL = "https://jumpit.saramin.co.kr/api/positions"

JUMPIT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://jumpit.saramin.co.kr/",
}


# ── Data containers ───────────────────────────────────────────────────────────

class JobPosting:
    """A finance job posting scraped from a job board."""
    def __init__(self, title: str, company: str, description: str,
                 requirements: str, url: str, source: str,
                 posted_at: datetime | None = None):
        self.title = title
        self.company = company
        self.description = description
        self.requirements = requirements
        self.url = url
        self.source = source
        self.posted_at = posted_at or datetime.utcnow()


class WantedProfile:
    """A talent pool profile scraped from Wanted."""
    def __init__(self, name: str, headline: str, current_company: str,
                 skills: list[str], experience_years: int | None,
                 profile_url: str, is_open: bool = True):
        self.name = name
        self.headline = headline
        self.current_company = current_company
        self.skills = skills
        self.experience_years = experience_years
        self.profile_url = profile_url
        self.is_open = is_open


# ── Wanted scrapers ───────────────────────────────────────────────────────────

def fetch_wanted_jobs(keyword: str, page: int = 0) -> list[dict]:
    """Fetch one page of Wanted job postings for a keyword."""
    params = {
        "query": keyword,
        "country": "kr",
        "job_sort": "job.latest_order",
        "limit": 20,
        "offset": page * 20,
    }
    try:
        with httpx.Client(headers=WANTED_HEADERS, timeout=15) as client:
            resp = client.get(_WANTED_JOBS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", {}).get("jobs", [])
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Wanted jobs error [%s, page %d]: %s", keyword, page, exc)
        return []


def fetch_wanted_talent(keyword: str, page: int = 0) -> list[dict]:
    """Fetch talent pool profiles from Wanted."""
    params = {
        "query": keyword,
        "offset": page * 20,
        "limit": 20,
    }
    try:
        with httpx.Client(headers=WANTED_HEADERS, timeout=15) as client:
            resp = client.get(_WANTED_TALENT_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", {}).get("profiles", [])
    except (httpx.HTTPError, ValueError) as exc:
        logger.debug("Wanted talent error [%s, page %d]: %s", keyword, page, exc)
        return []


def parse_wanted_job(raw: dict) -> JobPosting | None:
    """Parse a raw Wanted job dict into a JobPosting."""
    try:
        job = raw.get("job", raw)
        company = job.get("company", {}).get("name", "") or raw.get("company_name", "")
        title = job.get("position", "") or raw.get("title", "")
        detail = job.get("detail", {})
        description = detail.get("intro", "") or ""
        requirements = detail.get("requirement", "") or ""
        url = f"https://www.wanted.co.kr/wd/{job.get('id', '')}"
        posted_at_str = job.get("created_time", "")
        posted_at = None
        if posted_at_str:
            try:
                posted_at = datetime.fromisoformat(posted_at_str.replace("Z", "+00:00"))
            except ValueError:
                pass
        return JobPosting(
            title=title,
            company=company,
            description=description,
            requirements=requirements,
            url=url,
            source="wanted",
            posted_at=posted_at,
        )
    except Exception as exc:
        logger.debug("Failed to parse Wanted job: %s", exc)
        return None


def parse_wanted_profile(raw: dict) -> WantedProfile | None:
    """Parse a raw Wanted talent profile."""
    try:
        name = raw.get("name", "") or raw.get("username", "")
        headline = raw.get("headline", "") or raw.get("job_title", "")
        current = raw.get("current_company", {}) or {}
        company = current.get("name", "") if isinstance(current, dict) else str(current)
        skills = [s.get("name", "") for s in raw.get("skills", []) if isinstance(s, dict)]
        exp = raw.get("total_experience_months")
        exp_years = round(exp / 12) if exp else None
        profile_id = raw.get("id", "")
        profile_url = f"https://www.wanted.co.kr/profile/{profile_id}" if profile_id else ""
        is_open = raw.get("status", "") in ("AVAILABLE", "open", "active")
        return WantedProfile(
            name=name,
            headline=headline,
            current_company=company,
            skills=skills,
            experience_years=exp_years,
            profile_url=profile_url,
            is_open=is_open,
        )
    except Exception as exc:
        logger.debug("Failed to parse Wanted profile: %s", exc)
        return None


def scrape_wanted_jobs(delay: float = 1.5) -> Iterator[JobPosting]:
    """Yield JobPosting objects for all finance keywords."""
    for keyword in FINANCE_KEYWORDS:
        logger.info("Wanted jobs — keyword: %s", keyword)
        page = 0
        while True:
            items = fetch_wanted_jobs(keyword, page=page)
            if not items:
                break
            for raw in items:
                posting = parse_wanted_job(raw)
                if posting and posting.company:
                    yield posting
            if len(items) < 20:
                break
            page += 1
            time.sleep(delay)
        time.sleep(delay)


def scrape_wanted_talent(delay: float = 1.5) -> Iterator[WantedProfile]:
    """Yield WantedProfile objects from talent pool search."""
    for keyword in ["애널리스트", "펀드매니저", "투자운용", "equity analyst"]:
        logger.info("Wanted talent — keyword: %s", keyword)
        page = 0
        while True:
            items = fetch_wanted_talent(keyword, page=page)
            if not items:
                break
            for raw in items:
                profile = parse_wanted_profile(raw)
                if profile and profile.name:
                    yield profile
            if len(items) < 20:
                break
            page += 1
            time.sleep(delay)
        time.sleep(delay)


# ── Jumpit scraper ────────────────────────────────────────────────────────────

def fetch_jumpit_jobs(keyword: str, page: int = 1) -> list[dict]:
    """Fetch Jumpit job postings for a keyword."""
    params = {
        "keyword": keyword,
        "page": page,
        "sort": "created_at",
    }
    try:
        with httpx.Client(headers=JUMPIT_HEADERS, timeout=15) as client:
            resp = client.get(_JUMPIT_JOBS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            return data.get("result", {}).get("positions", [])
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Jumpit error [%s, page %d]: %s", keyword, page, exc)
        return []


def scrape_jumpit_jobs(delay: float = 1.5) -> Iterator[JobPosting]:
    """Yield JobPosting objects from Jumpit for finance keywords."""
    for keyword in ["애널리스트", "펀드매니저", "자산운용", "헤지펀드"]:
        logger.info("Jumpit jobs — keyword: %s", keyword)
        page = 1
        while True:
            items = fetch_jumpit_jobs(keyword, page=page)
            if not items:
                break
            for raw in items:
                company = raw.get("companyName", "") or raw.get("company", {}).get("name", "")
                title = raw.get("title", "") or raw.get("position", "")
                job_id = raw.get("id", "")
                url = f"https://jumpit.saramin.co.kr/position/{job_id}"
                if company and title:
                    yield JobPosting(
                        title=title,
                        company=company,
                        description=raw.get("description", ""),
                        requirements=raw.get("requirement", ""),
                        url=url,
                        source="jumpit",
                    )
            if len(items) < 20:
                break
            page += 1
            time.sleep(delay)
        time.sleep(delay)


# ── DB persistence ────────────────────────────────────────────────────────────

def _infer_firm_type(company: str) -> str:
    if any(k in company for k in ["운용", "투자자문"]):
        return "asset_manager"
    if any(k in company for k in ["증권"]):
        return "broker"
    if any(k in company for k in ["은행", "캐피탈"]):
        return "bank"
    if any(k in company for k in ["헤지", "파트너스"]):
        return "hedge_fund"
    return "other"


def upsert_from_profile(session, profile: WantedProfile) -> tuple[Candidate, bool]:
    """Insert or update Candidate from a Wanted talent profile."""
    existing = (
        session.query(Candidate)
        .filter_by(name=profile.name, current_firm=profile.current_company)
        .first()
    )
    if existing:
        if profile.profile_url and not existing.linkedin_url:
            existing.linkedin_url = profile.profile_url
        return existing, False

    candidate = Candidate(
        name=profile.name,
        current_firm=profile.current_company,
        firm_type=_infer_firm_type(profile.current_company),
        role=profile.headline,
        seniority=_infer_seniority_from_exp(profile.experience_years),
        linkedin_url=profile.profile_url,
        source="wanted",
        source_url=profile.profile_url,
        sourced_at=datetime.utcnow(),
        raw_data={
            "skills": profile.skills,
            "experience_years": profile.experience_years,
            "is_open": profile.is_open,
        },
    )
    session.add(candidate)
    return candidate, True


def _infer_seniority_from_exp(years: int | None) -> str:
    if years is None:
        return "analyst"
    if years >= 15:
        return "director"
    if years >= 8:
        return "vp"
    if years >= 4:
        return "associate"
    return "analyst"


# ── Main entry points ─────────────────────────────────────────────────────────

def run_jobs(dry_run: bool = False) -> dict:
    """
    Scrape job postings from Wanted + Jumpit.
    Returns counts of firms and postings found.
    Job postings are not stored as Candidates — they feed firm intelligence.
    Prints a summary of active hiring firms.
    """
    firms: dict[str, list[str]] = {}  # firm -> [titles]

    for posting in scrape_wanted_jobs():
        firms.setdefault(posting.company, []).append(posting.title)

    for posting in scrape_jumpit_jobs():
        firms.setdefault(posting.company, []).append(posting.title)

    if dry_run:
        for firm, titles in sorted(firms.items()):
            print(f"  {firm}: {', '.join(titles[:3])}")
    else:
        logger.info("Active hiring firms found: %d", len(firms))
        for firm, titles in firms.items():
            logger.info("  %s (%d postings)", firm, len(titles))

    return {"firms": len(firms), "postings": sum(len(v) for v in firms.values())}


def run_talent(dry_run: bool = False) -> tuple[int, int]:
    """
    Scrape open-to-work talent profiles from Wanted.
    Upserts into the candidate database.
    """
    from src.database.db import init_db
    init_db()

    session = get_session()
    created, updated = 0, 0
    try:
        for profile in scrape_wanted_talent():
            if dry_run:
                print(f"  {profile.name} | {profile.current_company} | {profile.headline}")
                continue
            _, is_new = upsert_from_profile(session, profile)
            if is_new:
                created += 1
            else:
                updated += 1
            if (created + updated) % 50 == 0:
                session.commit()
        session.commit()
        logger.info("Wanted talent scrape — created: %d, updated: %d", created, updated)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return created, updated
