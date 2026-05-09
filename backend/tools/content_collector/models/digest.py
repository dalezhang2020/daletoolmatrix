"""Daily Digest model — a frozen snapshot of each day's state.

Why snapshot (not FK to items/events)?
- items are GC'd after retention_days; digests must survive forever
- digests are append-only, one row per calendar day (America/New_York)
- rendering is just one SELECT + JSON decode
"""

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Integer,
    Index,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column

from ..database import SCHEMA, Base


class Digest(Base):
    __tablename__ = "digests"
    __table_args__ = (
        Index("ix_digests_date", "digest_date"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Calendar day in America/New_York — keeps identity stable across re-runs
    digest_date: Mapped[date] = mapped_column(Date, unique=True, nullable=False)

    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # AI-generated Chinese summary of the day's knowledge/tech items.
    # Only populated for the 08:00 ET morning digest; rolling today-digests
    # leave this NULL until the morning run.
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Frozen snapshots (full data, self-contained so we never need to join back)
    events_snapshot: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    items_snapshot: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    topics_snapshot: Mapped[list] = mapped_column(JSON, default=list, nullable=False)

    event_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    item_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    topic_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # {"zh": 18, "en": 7}
    lang_mix: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
