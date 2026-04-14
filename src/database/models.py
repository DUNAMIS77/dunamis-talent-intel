from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, JSON, Text, Enum as SAEnum
from sqlalchemy.orm import DeclarativeBase
import enum


class Base(DeclarativeBase):
    pass


class CandidateStatus(str, enum.Enum):
    unseen = "unseen"
    flagged = "flagged"
    approached = "approached"
    responded = "responded"
    passed = "passed"


class Candidate(Base):
    __tablename__ = "candidates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200))
    current_firm = Column(String(200))
    firm_type = Column(String(100))          # hedge_fund, asset_manager, broker, bank
    role = Column(String(200))
    seniority = Column(String(50))           # analyst, associate, vp, director, md, partner
    sector_coverage = Column(JSON)           # ["TMT", "Healthcare", ...]
    career_history = Column(JSON)            # [{"firm": ..., "role": ..., "years": ...}]
    education = Column(JSON)
    certifications = Column(JSON)            # ["CFA", "투자분석사", ...]
    contact_email = Column(String(200))
    linkedin_url = Column(String(500))
    source = Column(String(100))             # kofia, wanted, linkedin, fss
    source_url = Column(String(1000))
    sourced_at = Column(DateTime, default=datetime.utcnow)
    enriched_at = Column(DateTime)
    notes = Column(Text)
    status = Column(SAEnum(CandidateStatus), default=CandidateStatus.unseen)
    raw_data = Column(JSON)                  # original scraped payload

    def __repr__(self):
        return f"<Candidate {self.name} @ {self.current_firm}>"
