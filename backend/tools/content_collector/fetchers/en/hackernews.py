"""Hacker News — parses the front page HTML (same approach as newsnow).

No auth required. We use the HTML front page so we can grab the score,
which isn't always present in the Firebase API list endpoints.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from ..base import BaseFetcher, NewsItem
from ..http import fetch_text


_SCORE_RE = re.compile(r"\d+")


class HackerNewsFetcher(BaseFetcher):
    slug = "hackernews"
    name = "Hacker News"
    lang = "en"
    category = "tech"
    region = "global"
    interval_sec = 30 * 60
    weight = 1.0
    home_url = "https://news.ycombinator.com/"

    # Quality filter: drop low-signal submissions. 150 points means the
    # story hit the top tier of discussion — the "must-read" HN tier.
    # Below that are mostly experiments, niche shows, and not-yet-validated
    # submissions that would swamp a daily reading list.
    MIN_POINTS = 150

    async def fetch(self) -> list[NewsItem]:
        html = await fetch_text("https://news.ycombinator.com/")
        soup = BeautifulSoup(html, "html.parser")

        items: list[NewsItem] = []
        for rank, tr in enumerate(soup.select("tr.athing"), start=1):
            hn_id = tr.get("id") or ""
            title_link = tr.select_one(".titleline a")
            if not (hn_id and title_link):
                continue
            title = title_link.get_text(strip=True)
            href = title_link.get("href") or f"https://news.ycombinator.com/item?id={hn_id}"

            # Score lives in a separate <tr> keyed by the story id
            score_el = soup.select_one(f"#score_{hn_id}")
            score_val = None
            if score_el:
                m = _SCORE_RE.search(score_el.get_text())
                if m:
                    score_val = float(m.group(0))

            # Skip unscored posts and anything below the quality threshold.
            if score_val is None or score_val < self.MIN_POINTS:
                continue

            items.append(
                NewsItem(
                    external_id=hn_id,
                    title=title,
                    url=href,
                    hot_raw=score_val,
                    rank=rank,
                    metrics={"points": score_val} if score_val is not None else {},
                )
            )
        return items
