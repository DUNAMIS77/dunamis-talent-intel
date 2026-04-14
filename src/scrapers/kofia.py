"""
KOFIA (금융투자협회) scraper.

Targets the public member/professional registry at:
  https://www.kofia.or.kr  (회원사 및 전문인력 조회)

KOFIA publishes registered investment professionals (투자분석사, 펀드매니저,
운용전문인력 etc.) through their open data portal. This scraper pulls the
publicly accessible lists and normalises them into Candidate records.

Phase 1 scope:
  - 운용전문인력 (fund managers / portfolio managers)
  - 투자분석사 (investment analysts / research analysts)

KOFIA open-data endpoints (no auth required):
  POST https://www.kofia.or.kr/brd/m_14/view.do  (search form)
  The site also exposes an unofficial JSON API used by its own frontend.
  We target that JSON API for cleaner parsing.
"""

import httpx
import time
import logging
from datetime import datetime
from typing import Iterator

from src.database.models import Candidate
from src.database.db import get_session

logger = logging.getLogger(__name__)

# KOFIA open data base URL for professional registry
_KOFIA_SEARCH_URL = "https://www.kofia.or.kr/brd/m_14/list.do"
_KOFIA_ANALYST_URL = "https://www.kofia.or.kr/brd/m_113/list.do"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Referer": "https://www.kofia.or.kr/",
}

# Known large Korean asset managers, hedge funds, brokers to seed the search
SEED_FIRMS = [
    "삼성자산운용", "미래에셋자산운용", "KB자산운용", "신한자산운용",
    "한국투자신탁운용", "NH아문디자산운용", "하나자산운용", "키움투자자산운용",
    "트러스톤자산운용", "라임자산운용", "타임폴리오자산운용", "VIP자산운용",
    "브레인자산운용", "쿼드자산운용", "알파에셋자산운용",
    "삼성증권", "미래에셋증권", "KB증권", "NH투자증권", "한국투자증권",
    "키움증권", "신한투자증권", "하나증권", "메리츠증권",
    # Add more as needed
]


class KofiaRecord:
    """Parsed record from KOFIA registry."""
    def __init__(self, name: str, firm: str, role: str, license_type: str,
                 license_no: str | None = None, source_url: str = ""):
        self.name = name
        self.firm = firm
        self.role = role
        self.license_type = license_type
        self.license_no = license_no
        self.source_url = source_url


def _infer_firm_type(firm_name: str) -> str:
    """Guess firm type from Korean firm name."""
    if any(k in firm_name for k in ["운용", "투자자문"]):
        return "asset_manager"
    if any(k in firm_name for k in ["증권", "투자은행"]):
        return "broker"
    if any(k in firm_name for k in ["은행", "캐피탈"]):
        return "bank"
    if any(k in firm_name for k in ["헤지", "펀드", "파트너스"]):
        return "hedge_fund"
    return "other"


def _infer_seniority(role: str) -> str:
    """Infer seniority tier from Korean role title."""
    role_lower = role.lower()
    if any(k in role for k in ["대표", "CIO", "부사장", "사장", "전무"]):
        return "partner"
    if any(k in role for k in ["본부장", "상무", "이사", "운용역"]):
        return "director"
    if any(k in role for k in ["팀장", "수석", "책임", "부장"]):
        return "vp"
    if any(k in role for k in ["과장", "차장"]):
        return "associate"
    if any(k in role for k in ["애널리스트", "분석", "연구"]):
        return "analyst"
    return "analyst"


def scrape_kofia_page(firm_name: str, page: int = 1) -> list[KofiaRecord]:
    """
    Scrape one page of KOFIA professional registry for a given firm name.

    KOFIA's site uses a POST form. We replicate the minimal payload.
    Returns parsed records (may be empty if no results or parse fails).
    """
    payload = {
        "pageIndex": str(page),
        "pageUnit": "20",
        "searchCondition": "1",
        "searchKeyword": firm_name,
    }

    try:
        with httpx.Client(headers=HEADERS, timeout=15, follow_redirects=True) as client:
            resp = client.post(_KOFIA_SEARCH_URL, data=payload)
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("KOFIA HTTP error for firm %s: %s", firm_name, exc)
        return []

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(resp.text, "lxml")

    records: list[KofiaRecord] = []

    # KOFIA table rows — selector may need adjustment when site layout changes
    rows = soup.select("table.tbl_type1 tbody tr, table tbody tr")
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 3:
            continue
        # Typical columns: 이름, 소속회사, 자격구분, 자격번호
        name = cells[0] if cells else ""
        firm = cells[1] if len(cells) > 1 else firm_name
        license_type = cells[2] if len(cells) > 2 else ""
        license_no = cells[3] if len(cells) > 3 else None

        if not name or name in ("이름", "성명"):
            continue  # skip header row if captured

        records.append(KofiaRecord(
            name=name,
            firm=firm or firm_name,
            role=license_type,
            license_type=license_type,
            license_no=license_no,
            source_url=_KOFIA_SEARCH_URL,
        ))

    return records


def scrape_all_firms(delay_seconds: float = 1.5) -> Iterator[KofiaRecord]:
    """
    Iterate through all seed firms and yield KofiaRecord objects.
    Respects a polite crawl delay between requests.
    """
    for firm in SEED_FIRMS:
        logger.info("Scraping KOFIA for: %s", firm)
        page = 1
        while True:
            records = scrape_kofia_page(firm, page=page)
            if not records:
                break
            yield from records
            if len(records) < 20:
                break  # last page
            page += 1
            time.sleep(delay_seconds)
        time.sleep(delay_seconds)


def upsert_candidate(session, record: KofiaRecord) -> tuple[Candidate, bool]:
    """
    Insert or update a Candidate from a KofiaRecord.
    Returns (candidate, created).
    Deduplication key: name + current_firm.
    """
    existing = (
        session.query(Candidate)
        .filter_by(name=record.name, current_firm=record.firm)
        .first()
    )

    if existing:
        # Update certifications if new license type found
        certs = existing.certifications or []
        if record.license_type and record.license_type not in certs:
            certs.append(record.license_type)
            existing.certifications = certs
        return existing, False

    candidate = Candidate(
        name=record.name,
        current_firm=record.firm,
        firm_type=_infer_firm_type(record.firm),
        role=record.role,
        seniority=_infer_seniority(record.role),
        certifications=[record.license_type] if record.license_type else [],
        source="kofia",
        source_url=record.source_url,
        sourced_at=datetime.utcnow(),
        raw_data={"license_no": record.license_no, "license_type": record.license_type},
    )
    session.add(candidate)
    return candidate, True


def run(dry_run: bool = False):
    """
    Main entry point. Scrapes KOFIA and persists to DB.
    Set dry_run=True to print records without writing to DB.
    """
    from src.database.db import init_db
    init_db()

    session = get_session()
    created_count = 0
    updated_count = 0

    try:
        for record in scrape_all_firms():
            if dry_run:
                print(f"  {record.name} | {record.firm} | {record.license_type}")
                continue
            candidate, created = upsert_candidate(session, record)
            if created:
                created_count += 1
            else:
                updated_count += 1

            # Commit in batches of 50
            if (created_count + updated_count) % 50 == 0:
                session.commit()
                logger.info("Committed batch — created: %d, updated: %d",
                            created_count, updated_count)

        session.commit()
        logger.info("KOFIA scrape complete — created: %d, updated: %d",
                    created_count, updated_count)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return created_count, updated_count
