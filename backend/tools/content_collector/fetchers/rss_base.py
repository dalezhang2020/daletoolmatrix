"""RSS helper — a reusable base class for any source that's just a feed URL.

Uses feedparser (already in the Python ecosystem; zero deps beyond what's
already installed). Handles both RSS and Atom.

Subclass usage:

    class GoogleNewsAISearch(RSSFetcher):
        slug = "gnews_ai_search"
        name = "Google News · AI"
        lang = "en"
        category = "tech"
        region = "global"
        feed_url = "https://news.google.com/rss/search?q=AI&hl=en-US&gl=US&ceid=US:en"

Hot-score semantics: RSS rarely carries engagement data. We fall back to
rank-based scoring in services/score.py (first item = highest score).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import feedparser

from .base import BaseFetcher, NewsItem
from .http import fetch_text


# Google News appends " - Source Name" to every title; strip it for cleaner
# cards, but keep the source name as metadata.
_GNEWS_SOURCE_SUFFIX = re.compile(r"\s+-\s+([^-]+)$")


def _parse_date(entry) -> Optional[datetime]:
    # feedparser exposes .published_parsed as a struct_time; also handle raw
    # RFC-2822 strings (Google News style).
    if getattr(entry, "published_parsed", None):
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        except (TypeError, ValueError):
            pass
    raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
    if raw:
        try:
            return parsedate_to_datetime(raw).astimezone(timezone.utc)
        except (TypeError, ValueError):
            pass
    return None


class RSSFetcher(BaseFetcher):
    """Subclass and set `feed_url`. Optionally override `strip_source_suffix`
    (on by default — handy for Google News)."""

    feed_url: str = ""
    strip_source_suffix: bool = True
    limit: int = 30

    fetcher_type = "rss"

    async def fetch(self) -> list[NewsItem]:
        if not self.feed_url:
            raise RuntimeError(f"{self.slug}: feed_url is empty")

        # Use our httpx client so UA/retries are consistent across fetchers.
        xml = await fetch_text(self.feed_url)
        feed = feedparser.parse(xml)

        items: list[NewsItem] = []
        for rank, entry in enumerate(feed.entries[: self.limit], start=1):
            title = (entry.get("title") or "").strip()
            url = entry.get("link") or ""
            if not (title and url):
                continue

            source_name: Optional[str] = None
            if self.strip_source_suffix:
                m = _GNEWS_SOURCE_SUFFIX.search(title)
                if m:
                    source_name = m.group(1).strip()
                    title = _GNEWS_SOURCE_SUFFIX.sub("", title).strip()

            ext_id = entry.get("id") or entry.get("guid") or url
            pub = _parse_date(entry)

            items.append(
                NewsItem(
                    external_id=ext_id,
                    title=title,
                    url=url,
                    published_at=pub,
                    rank=rank,
                    metrics={"publisher": source_name} if source_name else {},
                    extra={"publisher": source_name} if source_name else {},
                )
            )
        return items
