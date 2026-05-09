"""Title translator — EN → ZH for knowledge/news items.

Design:
  - Only translates items from English sources (`source.lang = 'en'`)
  - Only translates items in 'knowledge' or 'news' categories (skips 'other')
  - Preserves proper nouns unchanged: brand names, product names, people,
    programming languages, tech terminology
  - Batches 30 items per LLM call to amortize system prompt cost
  - Idempotent: once `translated_at` is set, the row is skipped forever

Cost estimate: ~$0.001 per batch, ~$0.004/day steady-state once backlog
is drained.

Failure behaviour mirrors the classifier: on LLM error, rows are left NULL
so the next tick retries. Never writes a bogus translation.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Iterable

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import session_factory
from ..models.item import Item
from ..models.source import Source

logger = logging.getLogger(__name__)


_SYSTEM_PROMPT = (
    "You translate English tech-news content into concise, native-sounding "
    "Simplified Chinese for a personal dashboard. Each item has a title and "
    "optionally a summary — translate both.\n\n"
    "STRICT RULES:\n"
    "1. KEEP these kinds of terms in their original English — do NOT "
    "transliterate or translate:\n"
    "   - Product & brand names: Claude, GPT, ChatGPT, Gemini, Sora, "
    "Copilot, Cursor, DeepSeek, Llama, Mistral, Grok, Midjourney, Kimi, "
    "Windsurf, Replit, Figma, Notion, Linear, Vercel, Next.js, React, "
    "TypeScript, Python, Rust, Kubernetes, Docker, Redis, Postgres, "
    "MongoDB, iOS, Android, macOS, HarmonyOS, Wi-Fi\n"
    "   - Company names: OpenAI, Anthropic, Google, DeepMind, Meta, "
    "Microsoft, Apple, Amazon, Nvidia, NVIDIA, Tesla, SpaceX, Shopify, "
    "Stripe, Airbnb, Uber, GitHub, Hugging Face, Y Combinator, a16z\n"
    "   - People's names: Sam Altman, Elon Musk, Andrej Karpathy, "
    "Dario Amodei, Demis Hassabis (stay in English unless you know a "
    "widely-used Chinese name, in which case use it)\n"
    "   - Technical acronyms: AI, LLM, AGI, API, SDK, RAG, MCP, CVE, GPU, "
    "CPU, IPO, VC, SaaS, MVP, UI, UX, MoE, RL\n"
    "   - Version numbers, model names, benchmark names: GPT-5, "
    "Claude Opus 4.7, Gemini 2.5, MMLU, etc.\n\n"
    "2. Translate the SURROUNDING text into natural Chinese that a "
    "Chinese-speaking software engineer would actually use.\n\n"
    "3. Keep translations FAITHFUL — preserve the full meaning of the "
    "original. Do not summarize or compress. If the original is long, "
    "the translation should be similarly complete.\n\n"
    "4. Do NOT translate URLs, code, file paths, or handles like @swyx.\n\n"
    "5. Return ONLY a JSON object of this shape, no prose, no markdown:\n"
    "   {\"r\": [{\"i\": <index>, \"t\": \"<title_zh>\", \"s\": \"<summary_zh or empty string>\"}]}\n"
    "   - `t` is required. If the title has no natural translation "
    "(version tags like 'llm 0.32a0'), return the English unchanged.\n"
    "   - `s` is optional. Omit or leave as empty string when the input "
    "had no summary, or when the summary would just be machine noise.\n"
    "   - Omit whole entries for items you can't translate (they'll retry).\n\n"
    "EXAMPLES:\n"
    "  Input:\n"
    "    0. TITLE: Anthropic signs $1.8 billion AI cloud deal with Akamai\n"
    "       SUMMARY: Anthropic today announced a multi-year partnership with Akamai...\n"
    "  Output entry:\n"
    "    {\"i\": 0, \"t\": \"Anthropic 与 Akamai 签署 18 亿美元 AI 云服务合作\", "
    "\"s\": \"Anthropic 今天宣布与 Akamai 达成多年合作…\"}\n"
)


async def _llm_translate_batch(
    entries: list[tuple[str, str | None, str | None]]
) -> list[tuple[str | None, str | None]]:
    """Translate (title, summary, source_slug) triples in one LLM call.

    Returns a list aligned with `entries` of (title_zh, summary_zh). Either
    item can be None when the LLM didn't provide one. On transport failure,
    returns all-None pairs."""
    if not entries:
        return []

    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    model = os.getenv("AZURE_OPENAI_CHAT_MODEL", "gpt-4o-mini")
    if not (endpoint and api_key):
        logger.warning(
            "content_collector: translator skipped (missing AZURE_OPENAI_*)"
        )
        return [(None, None)] * len(entries)

    url = endpoint.rstrip("/") + "/chat/completions"
    # Compact input format keeps token usage low.
    # Slugs whose summaries are raw transcripts — skip to protect token budget.
    SKIP_SUMMARY_SLUGS: frozenset[str] = frozenset({"follow_builders_podcasts"})
    lines: list[str] = []
    for i, (title, summary, slug) in enumerate(entries):
        lines.append(f"{i}. TITLE: {title[:250]}")
        if summary and slug not in SKIP_SUMMARY_SLUGS:
            # 2000 chars covers most summaries fully without token risk.
            lines.append(f"   SUMMARY: {summary[:2000]}")
    user_payload = "\n".join(lines)

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            body = {
                "model": model,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_payload},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.1,
                # More tokens: summaries roughly double output size
                "max_completion_tokens": 5000,
            }
            resp = await client.post(
                url,
                headers={
                    "api-key": api_key,
                    "Content-Type": "application/json",
                },
                json=body,
            )
            if resp.status_code >= 400:
                logger.warning(
                    "content_collector: translator HTTP %d — %s",
                    resp.status_code,
                    resp.text[:400],
                )
                return [(None, None)] * len(entries)
            content = resp.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            results = parsed.get("r") or []
            if not results:
                logger.warning(
                    "content_collector: translator returned empty results. "
                    "content[:400]=%s",
                    (content or "")[:400],
                )
    except Exception as e:  # pragma: no cover
        logger.warning("content_collector: translator error: %s", e)
        return [(None, None)] * len(entries)

    out: list[tuple[str | None, str | None]] = [(None, None)] * len(entries)
    for r in results:
        try:
            idx = int(r["i"])
            if not (0 <= idx < len(entries)):
                continue
            title_zh = str(r.get("t") or "").strip() or None
            summary_zh_raw = r.get("s")
            summary_zh = str(summary_zh_raw).strip() if summary_zh_raw else None
            # Cap to column lengths.
            if title_zh:
                title_zh = title_zh[:500]
            if summary_zh:
                summary_zh = summary_zh[:2000]
            out[idx] = (title_zh, summary_zh)
        except (KeyError, ValueError, TypeError):
            continue
    return out


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def translate_backlog(
    batch_size: int = 30,
    llm_cap: int | None = 30,
) -> dict:
    """Translate one batch of untranslated EN knowledge/news items.

    Target rows:
      - source is English
      - category is 'news' or 'knowledge'
      - EITHER title_zh is NULL (never translated)
      - OR summary is non-empty AND summary_zh is NULL (title translated
        before we started doing summaries — retry to fill the gap)

    `llm_cap` bounds how many items per call actually hit the LLM.
    """
    factory = session_factory()

    async with factory() as session:
        # Needs title_zh; or has a summary we haven't translated yet.
        from sqlalchemy import and_, or_

        needs_work = or_(
            Item.title_zh.is_(None),
            and_(
                Item.summary.is_not(None),
                Item.summary != "",
                Item.summary_zh.is_(None),
            ),
        )

        q = (
            select(Item, Source.slug.label("source_slug"))
            .join(Source, Source.id == Item.source_id)
            .where(needs_work)
            .where(Source.lang == "en")
            .where(Item.category.in_(["news", "knowledge"]))
            .order_by(Item.first_seen_at.desc())
            .limit(batch_size)
        )
        rows = (await session.execute(q)).all()
        items: list[Item] = [r[0] for r in rows]
        slugs: list[str] = [r[1] for r in rows]

        if not items:
            return {"processed": 0, "translated": 0, "deferred": 0}

        to_translate = items if llm_cap is None else items[: max(0, llm_cap)]
        to_slugs = slugs[: len(to_translate)]
        deferred = items[len(to_translate):]

        translated_count = 0
        if to_translate:
            predictions = await _llm_translate_batch(
                [(it.title, it.summary, slug)
                 for it, slug in zip(to_translate, to_slugs)]
            )
            now = datetime.now(timezone.utc)
            for it, (title_zh, summary_zh) in zip(to_translate, predictions):
                if title_zh is None and summary_zh is None:
                    continue  # full miss — retry next tick
                if title_zh:
                    it.title_zh = title_zh
                if summary_zh:
                    it.summary_zh = summary_zh
                it.translated_at = now
                translated_count += 1

        await session.commit()

    result = {
        "processed": len(items),
        "translated": translated_count,
        "deferred": len(deferred) + (len(to_translate) - translated_count),
    }
    logger.info("content_collector: translate_backlog %s", result)
    return result


async def translate_drain(
    max_batches: int = 50, llm_cap_per_batch: int = 30
) -> dict:
    """Drain the translation backlog in a loop, capped per-batch."""
    total = {"processed": 0, "translated": 0, "deferred": 0}
    for _ in range(max_batches):
        r = await translate_backlog(llm_cap=llm_cap_per_batch)
        for k in total:
            total[k] += r.get(k, 0)
        if r.get("processed", 0) == 0:
            break
    return total
