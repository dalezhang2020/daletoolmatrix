"""Follow Builders — three fetchers reading a centrally-maintained feed.

Source: https://github.com/zarazhangrui/follow-builders

That repo runs a daily GitHub Action that scrapes:
  - X/Twitter posts from ~25 curated AI builders (24h window)
  - Blog posts from Anthropic Engineering + Claude Blog (72h window)
  - Podcast episodes from 6 AI podcasts with full YouTube transcripts (336h window)

… then commits the JSON to `main`. We just read the raw files — no API keys,
no rate limits, no maintenance on our side. Re-ingesting the same items is
idempotent (our (source_id, external_id) unique constraint takes care of it),
so fetching every 2h is fine even though the upstream only refreshes daily.

All three map to category=knowledge via SOURCE_FORCED_CATEGORY in the
classifier, so these items skip both rules and LLM.
"""

from __future__ import annotations

import hashlib
from datetime import datetime

from ..base import BaseFetcher, NewsItem
from ..http import fetch_json

_BASE = "https://raw.githubusercontent.com/zarazhangrui/follow-builders/main"


def _iso_to_dt(s: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp like '2026-05-08T07:59:14.772Z' safely."""
    if not s:
        return None
    try:
        # Python's fromisoformat accepts the trailing Z since 3.11
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _digest(*parts: str) -> str:
    """Stable 16-char id built from the concatenation of the given parts."""
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(b"|")
    return h.hexdigest()[:16]


# ---------------------------------------------------------------------------
# X / Twitter — 25 AI builders' last-24h posts
# ---------------------------------------------------------------------------


class FollowBuildersXFetcher(BaseFetcher):
    slug = "follow_builders_x"
    name = "Follow Builders · X"
    lang = "en"
    category = "tech"
    region = "global"
    interval_sec = 2 * 60 * 60  # upstream refreshes daily; 2h is safe
    weight = 1.1
    home_url = "https://github.com/zarazhangrui/follow-builders"

    async def fetch(self) -> list[NewsItem]:
        data = await fetch_json(f"{_BASE}/feed-x.json")
        out: list[NewsItem] = []

        for author in (data or {}).get("x", []) or []:
            handle = author.get("handle") or ""
            display_name = author.get("name") or handle
            for tw in author.get("tweets", []) or []:
                tid = tw.get("id")
                url = tw.get("url")
                text = (tw.get("text") or "").strip()
                if not (tid and url and text):
                    continue

                # Skip obvious low-signal posts: too short to matter, or
                # tweets with zero engagement and no text body.
                stripped = text.strip()
                engagement = (
                    int(tw.get("likes") or 0)
                    + int(tw.get("retweets") or 0)
                    + int(tw.get("replies") or 0)
                )
                if len(stripped) < 60 and engagement < 20:
                    continue

                likes = int(tw.get("likes") or 0)
                retweets = int(tw.get("retweets") or 0)
                replies = int(tw.get("replies") or 0)
                # Engagement as the hot_raw signal. Retweets are worth more
                # than likes (higher commitment), replies too (conversation).
                hot_raw = float(likes + retweets * 3 + replies * 2)

                # Title = "@handle: first line of tweet, trimmed"
                first_line = text.splitlines()[0][:160]
                title = f"@{handle}: {first_line}"

                out.append(
                    NewsItem(
                        external_id=str(tid),
                        title=title,
                        url=url,
                        author=display_name,
                        summary=text[:500],
                        published_at=_iso_to_dt(tw.get("createdAt")),
                        hot_raw=hot_raw,
                        metrics={
                            "likes": likes,
                            "retweets": retweets,
                            "replies": replies,
                            "is_quote": bool(tw.get("isQuote")),
                        },
                    )
                )

        # Sort newest-first so snapshot rank reflects recency within this batch
        out.sort(
            key=lambda n: n.published_at or datetime.min, reverse=True
        )
        for rank, item in enumerate(out, start=1):
            item.rank = rank
        return out


# ---------------------------------------------------------------------------
# Blogs — Anthropic Engineering + Claude Blog, 72h window
# ---------------------------------------------------------------------------


class FollowBuildersBlogsFetcher(BaseFetcher):
    slug = "follow_builders_blogs"
    name = "Follow Builders · Blogs"
    lang = "en"
    category = "tech"
    region = "global"
    interval_sec = 2 * 60 * 60
    weight = 1.3
    home_url = "https://github.com/zarazhangrui/follow-builders"

    async def fetch(self) -> list[NewsItem]:
        data = await fetch_json(f"{_BASE}/feed-blogs.json")
        out: list[NewsItem] = []

        for post in (data or {}).get("blogs", []) or []:
            title = (post.get("title") or "").strip()
            url = post.get("url")
            if not (title and url):
                continue

            # Prefer a stable native id if upstream provides one; fall back
            # to a hash of (url, title) so re-exports don't create dupes.
            ext_id = str(post.get("guid") or post.get("id") or _digest(url, title))

            out.append(
                NewsItem(
                    external_id=ext_id,
                    title=title,
                    url=url,
                    author=post.get("source") or post.get("blog"),
                    summary=(post.get("excerpt") or post.get("content") or "")[:500],
                    published_at=_iso_to_dt(post.get("publishedAt")),
                    # No upstream engagement numbers. Fall back to rank-based
                    # scoring in normalize_batch via a placeholder rank.
                    hot_raw=None,
                )
            )

        # Sort newest-first and rank so score.py's rank fallback is meaningful
        out.sort(
            key=lambda n: n.published_at or datetime.min, reverse=True
        )
        for rank, item in enumerate(out, start=1):
            item.rank = rank
        return out


# ---------------------------------------------------------------------------
# Podcasts — AI podcasts' new episodes (336h window)
# ---------------------------------------------------------------------------


class FollowBuildersPodcastsFetcher(BaseFetcher):
    slug = "follow_builders_podcasts"
    name = "Follow Builders · Podcasts"
    lang = "en"
    category = "tech"
    region = "global"
    interval_sec = 6 * 60 * 60  # new episodes arrive at most daily
    weight = 1.2
    home_url = "https://github.com/zarazhangrui/follow-builders"

    async def fetch(self) -> list[NewsItem]:
        data = await fetch_json(f"{_BASE}/feed-podcasts.json")
        out: list[NewsItem] = []

        for ep in (data or {}).get("podcasts", []) or []:
            title = (ep.get("title") or "").strip()
            url = ep.get("url")
            if not (title and url):
                continue

            ext_id = str(ep.get("guid") or _digest(url, title))
            show = ep.get("name") or "AI podcast"
            # Use first 280 chars of transcript as summary so the classifier
            # and the UI preview both have something substantive to show.
            transcript = (ep.get("transcript") or "").strip()

            out.append(
                NewsItem(
                    external_id=ext_id,
                    title=f"{show}: {title}",
                    url=url,
                    author=show,
                    summary=transcript[:500] if transcript else None,
                    published_at=_iso_to_dt(ep.get("publishedAt")),
                    hot_raw=None,
                    metrics={
                        "has_transcript": bool(transcript),
                    },
                )
            )

        out.sort(
            key=lambda n: n.published_at or datetime.min, reverse=True
        )
        for rank, item in enumerate(out, start=1):
            item.rank = rank
        return out
