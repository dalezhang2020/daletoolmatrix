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
    "You translate English tech-news headlines into concise, native-sounding "
    "Simplified Chinese for a personal dashboard.\n\n"
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
    "CPU, IPO, VC, SaaS, MVP, UI, UX\n"
    "   - Version numbers, model names, benchmark names: GPT-5, "
    "Claude Opus 4.7, Gemini 2.5, MMLU, etc.\n\n"
    "2. Translate the SURROUNDING text into natural Chinese that a "
    "Chinese-speaking software engineer would actually use.\n\n"
    "3. Keep the translation CONCISE — aim for the same length or shorter "
    "than the English original.\n\n"
    "4. Do NOT translate URLs, code, file paths, or handles like @swyx.\n\n"
    "5. Return ONLY a JSON object of this shape, no prose, no markdown:\n"
    "   {\"r\": [{\"i\": <index>, \"t\": \"<title_zh>\"}]}\n"
    "   Omit entries for items you can't translate (they'll retry later).\n\n"
    "EXAMPLES:\n"
    "  EN: 'Anthropic signs $1.8 billion AI cloud deal with Akamai'\n"
    "  ZH: 'Anthropic 与 Akamai 签署 18 亿美元 AI 云服务合作'\n\n"
    "  EN: '@sama: we'd like to help companies secure themselves'\n"
    "  ZH: '@sama：我们希望帮助企业做好自身安全'\n\n"
    "  EN: 'llm-gemini 0.31'\n"
    "  ZH: 'llm-gemini 0.31'  (version tag, no natural translation)\n"
)


async def _llm_translate_batch(titles: list[str]) -> list[str | None]:
    """Return a list of translations aligned with `titles`; None = couldn't
    translate this one. On transport failure, returns all-None."""
    if not titles:
        return []

    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    model = os.getenv("AZURE_OPENAI_CHAT_MODEL", "gpt-4o-mini")
    if not (endpoint and api_key):
        logger.warning(
            "content_collector: translator skipped (missing AZURE_OPENAI_*)"
        )
        return [None] * len(titles)

    url = endpoint.rstrip("/") + "/chat/completions"
    user_payload = "\n".join(f"{i}. {t[:250]}" for i, t in enumerate(titles))

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            body = {
                "model": model,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_payload},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.1,
                "max_completion_tokens": 2500,
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
                return [None] * len(titles)
            content = resp.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            results = parsed.get("r") or []
    except Exception as e:  # pragma: no cover
        logger.warning("content_collector: translator error: %s", e)
        return [None] * len(titles)

    out: list[str | None] = [None] * len(titles)
    for r in results:
        try:
            idx = int(r["i"])
            text = str(r["t"]).strip()
            if 0 <= idx < len(titles) and text and text != titles[idx]:
                out[idx] = text[:500]  # cap to column size
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

    `llm_cap` bounds how many items per call actually hit the LLM (for
    token safety). Defaults to batch_size.
    """
    factory = session_factory()

    async with factory() as session:
        # Pull candidates: English source + category in (news, knowledge) +
        # not yet translated. Pick recently-seen first so the dashboard gets
        # the freshest headlines translated first.
        q = (
            select(Item)
            .join(Source, Source.id == Item.source_id)
            .where(Item.title_zh.is_(None))
            .where(Source.lang == "en")
            .where(Item.category.in_(["news", "knowledge"]))
            .order_by(Item.first_seen_at.desc())
            .limit(batch_size)
        )
        items: list[Item] = list((await session.execute(q)).scalars().all())

        if not items:
            return {"processed": 0, "translated": 0, "deferred": 0}

        to_translate = items if llm_cap is None else items[: max(0, llm_cap)]
        deferred = items[len(to_translate):]

        translated_count = 0
        if to_translate:
            predictions = await _llm_translate_batch(
                [it.title for it in to_translate]
            )
            now = datetime.now(timezone.utc)
            for it, pred in zip(to_translate, predictions):
                if pred:
                    it.title_zh = pred
                    it.translated_at = now
                    translated_count += 1
                # Items without a prediction stay NULL → retried next tick.

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
