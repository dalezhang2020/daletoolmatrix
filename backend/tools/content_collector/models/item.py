"""Item + ItemSnapshot models.

Item         = a deduplicated post (identified by source + external_id)
ItemSnapshot = its engagement metrics captured at fetch time, so we can track
               how hotness evolves over the 7-day window.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column

from ..database import SCHEMA, Base


class Item(Base):
    __tablename__ = "items"
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uq_items_source_external"),
        Index("ix_items_first_seen_at", "first_seen_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey(f"{SCHEMA}.sources.id", ondelete="CASCADE"), nullable=False, index=True
    )

    external_id: Mapped[str] = mapped_column(String(512), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    mobile_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    author: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cover: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    # Free-form, source-specific extras (icons, labels, etc.)
    extra: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)

    # Content category (filled by the classifier service). NULL = pending.
    category: Mapped[Optional[str]] = mapped_column(String(24), nullable=True, index=True)
    # 'rule' | 'llm' | 'manual'  — for debugging and later re-classification
    category_source: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    categorized_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Chinese translation (filled by the translator service).
    # Applies only to English-source items; Chinese items leave these NULL.
    # Proper nouns (brand names, product names, people) are preserved as-is
    # per the translator system prompt.
    title_zh: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    summary_zh: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    translated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class ItemSnapshot(Base):
    __tablename__ = "item_snapshots"
    __table_args__ = (
        Index("ix_item_snapshots_item_captured", "item_id", "captured_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    item_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey(f"{SCHEMA}.items.id", ondelete="CASCADE"), nullable=False
    )

    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True
    )

    # hot_raw = whatever the source gave us (upvotes, 热度值, 人气, etc.)
    hot_raw: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # hot_score = normalized 0-100 so we can compare across sources
    hot_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

    rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    metrics: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
