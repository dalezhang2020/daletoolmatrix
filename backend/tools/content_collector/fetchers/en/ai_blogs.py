"""High-signal AI / tech blogs via RSS.

All zero-auth, stable feeds. Frequencies are conservative (many of these post
1-3×/day at most).
"""

from __future__ import annotations

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


class HuggingFaceDailyPapers(RSSFetcher):
    slug = "hf_daily_papers"
    name = "Hugging Face Daily Papers"
    lang = "en"
    category = "tech"
    region = "global"
    interval_sec = 6 * 60 * 60
    weight = 1.2
    # HF has no official RSS for papers; use the community-maintained one.
    feed_url = "https://jamesg.blog/hf-papers.xml"
    home_url = "https://huggingface.co/papers"
    strip_source_suffix = False


class Stratechery(RSSFetcher):
    slug = "stratechery"
    name = "Stratechery"
    lang = "en"
    category = "tech"
    region = "global"
    interval_sec = 6 * 60 * 60
    weight = 1.1  # free posts only; paywalled content is partial in feed
    feed_url = "https://stratechery.com/feed/"
    home_url = "https://stratechery.com"
    strip_source_suffix = False
