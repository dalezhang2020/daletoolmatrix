"""Google News via RSS — AI search query only.

Google News RSS carries no engagement data; ingest falls back to rank-based
scoring (first item = hottest). The `<source>` tag per item is stripped from
the headline (e.g. "...  - NYT") and kept as metadata for display.

Why only gnews_ai_search: the US-top and Technology topic feeds were mostly
entertainment/sports/gaming noise once we looked at real data; this targeted
search query produces much higher signal for AI coverage specifically.
"""

from __future__ import annotations

from ..rss_base import RSSFetcher


class GoogleNewsAISearch(RSSFetcher):
    slug = "gnews_ai_search"
    name = "Google News · AI"
    lang = "en"
    category = "tech"
    region = "global"
    interval_sec = 60 * 60
    weight = 1.2
    feed_url = (
        "https://news.google.com/rss/search"
        "?q=AI+OR+LLM+OR+OpenAI+OR+Anthropic+OR+%22artificial+intelligence%22"
        "&hl=en-US&gl=US&ceid=US:en"
    )
    home_url = "https://news.google.com"
