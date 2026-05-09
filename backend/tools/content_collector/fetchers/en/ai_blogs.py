"""High-signal AI / tech blogs via RSS.

All zero-auth, stable feeds. Frequencies are conservative (many of these post
1-3×/day at most).
"""

from __future__ import annotations

from datetime import datetime

from ..base import BaseFetcher, NewsItem
from ..http import fetch_json
from ..rss_base import RSSFetcher


class SimonWillisonFeed(RSSFetcher):
    slug = "simon_willison"
    name = "Simon Willison's Blog"
    lang = "en"
    category = "tech"
    region = "global"
    interval_sec = 2 * 60 * 60
    weight = 1.3  # one of the best AI observers online
    feed_url = "https://simonwillison.net/atom/everything/"
    home_url = "https://simonwillison.net"
    strip_source_suffix = False


class HuggingFaceDailyPapers(BaseFetcher):
    """Hugging Face Daily Papers via the site's own JSON API.

    The old implementation pointed at jamesg.blog's RSS proxy which routes
    through granary.io — that proxy has been flaky (502s). The official
    `/api/daily_papers` endpoint is public, no auth needed, and returns
    richer data (upvotes, discussion count) we can use for hot_raw.
    """

    slug = "hf_daily_papers"
    name = "Hugging Face Daily Papers"
    lang = "en"
    category = "tech"
    region = "global"
    fetcher_type = "native"
    interval_sec = 6 * 60 * 60
    weight = 1.2
    home_url = "https://huggingface.co/papers"

    _API = "https://huggingface.co/api/daily_papers"

    async def fetch(self) -> list[NewsItem]:
        data = await fetch_json(self._API, params={"limit": 30})
        out: list[NewsItem] = []
        for rank, row in enumerate(data or [], start=1):
            paper = (row or {}).get("paper") or {}
            pid = paper.get("id")
            title = (row.get("title") or paper.get("title") or "").strip()
            if not (pid and title):
                continue

            url = f"https://huggingface.co/papers/{pid}"
            # Engagement: upvotes + comments. Comments weigh more than
            # upvotes since they imply real discussion on the paper.
            upvotes = int(paper.get("upvotes") or 0)
            comments = int(row.get("numComments") or 0)
            hot_raw = float(upvotes + comments * 2)

            published_at: datetime | None = None
            pub_str = row.get("publishedAt") or paper.get("publishedAt")
            if pub_str:
                try:
                    published_at = datetime.fromisoformat(
                        pub_str.replace("Z", "+00:00")
                    )
                except ValueError:
                    published_at = None

            # Prefer the AI-generated one-sentence summary when present;
            # fall back to the first 400 chars of the abstract.
            summary = (
                paper.get("ai_summary")
                or (row.get("summary") or paper.get("summary") or "")[:400]
            )

            authors = paper.get("authors") or []
            first_author = (authors[0] or {}).get("name") if authors else None

            out.append(
                NewsItem(
                    external_id=str(pid),
                    title=title,
                    url=url,
                    author=first_author,
                    summary=summary,
                    published_at=published_at,
                    hot_raw=hot_raw,
                    rank=rank,
                    metrics={
                        "upvotes": upvotes,
                        "comments": comments,
                    },
                    extra={
                        "arxiv_id": pid,
                        "github_repo": paper.get("githubRepo"),
                    },
                )
            )
        return out
