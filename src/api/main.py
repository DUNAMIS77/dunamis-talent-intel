"""Minimal FastAPI dashboard for browsing the candidate database."""

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from sqlalchemy import or_
import json

from src.database.db import get_session, init_db
from src.database.models import Candidate, CandidateStatus

app = FastAPI(title="Dunamis Talent Intel")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/candidates")
def list_candidates(
    status: str | None = None,
    firm_type: str | None = None,
    sector: str | None = None,
    seniority: str | None = None,
    q: str | None = None,
    limit: int = Query(50, le=200),
    offset: int = 0,
):
    """Search and filter candidates."""
    session = get_session()
    try:
        query = session.query(Candidate)

        if status:
            query = query.filter(Candidate.status == status)
        if firm_type:
            query = query.filter(Candidate.firm_type == firm_type)
        if seniority:
            query = query.filter(Candidate.seniority == seniority)
        if q:
            query = query.filter(
                or_(
                    Candidate.name.ilike(f"%{q}%"),
                    Candidate.current_firm.ilike(f"%{q}%"),
                    Candidate.role.ilike(f"%{q}%"),
                )
            )

        total = query.count()
        candidates = query.order_by(Candidate.sourced_at.desc()).offset(offset).limit(limit).all()

        return {
            "total": total,
            "candidates": [_to_dict(c) for c in candidates],
        }
    finally:
        session.close()


@app.patch("/candidates/{candidate_id}/status")
def update_status(candidate_id: int, status: CandidateStatus):
    session = get_session()
    try:
        candidate = session.query(Candidate).get(candidate_id)
        if not candidate:
            from fastapi import HTTPException
            raise HTTPException(404, "Candidate not found")
        candidate.status = status
        session.commit()
        return {"id": candidate_id, "status": status}
    finally:
        session.close()


@app.get("/stats")
def stats():
    session = get_session()
    try:
        total = session.query(Candidate).count()
        enriched = session.query(Candidate).filter(Candidate.enriched_at.isnot(None)).count()
        by_status = {}
        for s in CandidateStatus:
            by_status[s.value] = session.query(Candidate).filter(
                Candidate.status == s
            ).count()
        by_firm_type = {}
        for ft in ["asset_manager", "broker", "hedge_fund", "bank", "other"]:
            by_firm_type[ft] = session.query(Candidate).filter(
                Candidate.firm_type == ft
            ).count()
        return {
            "total": total,
            "enriched": enriched,
            "by_status": by_status,
            "by_firm_type": by_firm_type,
        }
    finally:
        session.close()


def _to_dict(c: Candidate) -> dict:
    return {
        "id": c.id,
        "name": c.name,
        "current_firm": c.current_firm,
        "firm_type": c.firm_type,
        "role": c.role,
        "seniority": c.seniority,
        "sector_coverage": c.sector_coverage,
        "certifications": c.certifications,
        "status": c.status,
        "source": c.source,
        "sourced_at": c.sourced_at.isoformat() if c.sourced_at else None,
        "enriched_at": c.enriched_at.isoformat() if c.enriched_at else None,
        "contact_email": c.contact_email,
        "linkedin_url": c.linkedin_url,
        "notes": c.notes,
    }
