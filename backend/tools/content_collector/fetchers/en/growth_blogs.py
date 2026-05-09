"""Growth / psychology / philosophy blogs — fills the 'growth' category
which is near-empty from the other sources."""

from __future__ import annotations

from ..rss_base import RSSFetcher


class FarnamStreet(RSSFetcher):
    slug = "farnam_street"
    name = "Farnam Street"
    lang = "en"
    category = "world"
    region = "global"
    interval_sec = 12 * 60 * 60
    weight = 1.2
    feed_url = "https://fs.blog/feed/"
    home_url = "https://fs.blog"
    strip_source_suffix = False


class NessLabs(RSSFetcher):
    """Anne-Laure Le Cunff's newsletter — mindful productivity, cognitive
    science, self-improvement. 2-3 posts/week, consistently high quality.
    Replaces Astral Codex Ten which had too much operational noise."""

    slug = "ness_labs"
    name = "Ness Labs"
    lang = "en"
    category = "world"
    region = "global"
    interval_sec = 12 * 60 * 60
    weight = 1.3
    feed_url = "https://nesslabs.com/feed"
    home_url = "https://nesslabs.com"
    strip_source_suffix = False
