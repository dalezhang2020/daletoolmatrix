"""IT 之家 24h 热榜 — m.ithome.com/rankm (mobile page, lightest HTML)."""

from __future__ import annotations

from bs4 import BeautifulSoup

from ..base import BaseFetcher, NewsItem
from ..http import fetch_text


class ITHomeFetcher(BaseFetcher):
    slug = "ithome"
    name = "IT 之家 24h 热榜"
    lang = "zh"
    category = "tech"
    region = "cn"
    interval_sec = 60 * 60
    weight = 0.9
    home_url = "https://www.ithome.com/rankm/"

    async def fetch(self) -> list[NewsItem]:
        html = await fetch_text("https://m.ithome.com/rankm/")
        soup = BeautifulSoup(html, "html.parser")

        items: list[NewsItem] = []
        # .plc-title is the <p> holding the headline; wrapping <a> holds the URL.
        for rank, p in enumerate(soup.select(".plc-title"), start=1):
            title = p.get_text(strip=True)
            if not title:
                continue

            a = p.find_parent("a")
            href = a.get("href") if a else None
            if not href:
                # Some rows put the anchor as a sibling/child instead
                a_alt = p.find("a") or (p.parent.find("a") if p.parent else None)
                href = a_alt.get("href") if a_alt else None
            if not href:
                continue
            if href.startswith("//"):
                href = "https:" + href

            items.append(
                NewsItem(
                    external_id=href,
                    title=title,
                    url=href,
                    mobile_url=href,
                    rank=rank,
                )
            )
            # Only keep the top 10 — IT 之家's hot list drops sharply
            # after rank 10 and the tail is mostly filler.
            if rank >= 10:
                break
        return items
