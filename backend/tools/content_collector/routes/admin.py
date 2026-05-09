"""Admin endpoints: manually trigger a fetch, useful for dev and troubleshooting.

These are intentionally not behind auth right now — add basic auth before
exposing publicly.
"""

from fastapi import APIRouter, HTTPException, Query

from ..fetchers.registry import get_fetcher, get_registry
from ..services.categorize import categorize_backlog, reclassify_all_with_rules
from ..services.cluster import recluster_all
from ..services.digest import generate_digest
from ..services.events import detect_events
from ..services.ingest import run_source
from ..services.translate import translate_backlog, translate_drain

router = APIRouter()


@router.post("/fetch/{slug}")
async def trigger_fetch(slug: str):
    if slug == "all":
        results = []
        for s in get_registry().keys():
            results.append(await run_source(s))
        return {"total": len(results), "results": results}

    if not get_fetcher(slug):
        raise HTTPException(404, f"unknown source: {slug}")
    return await run_source(slug)


@router.post("/recluster")
async def trigger_recluster(days: int = 7):
    return await recluster_all(days=days)


@router.post("/detect-events")
async def trigger_detect_events():
    return await detect_events()


@router.post("/generate-digest")
async def trigger_generate_digest(
    date: str | None = Query(None, description="YYYY-MM-DD; default = yesterday in ET"),
):
    target = None
    if date:
        from datetime import date as _date

        try:
            target = _date.fromisoformat(date)
        except ValueError:
            raise HTTPException(400, "invalid date — use YYYY-MM-DD")
    return await generate_digest(target)


@router.post("/categorize")
async def trigger_categorize(
    batch_size: int = 50,
    use_llm: bool = True,
):
    """Classify one batch of uncategorized items."""
    return await categorize_backlog(batch_size=batch_size, use_llm=use_llm)


@router.post("/categorize-drain")
async def trigger_categorize_drain(
    use_llm: bool = True,
    max_batches: int = 40,
    llm_cap_per_batch: int = 50,
):
    """Repeatedly classify until the backlog is empty (or max_batches hit).

    `llm_cap_per_batch` limits how many items per batch are allowed to hit
    the LLM — set to 0 to stop all LLM calls for this drain (useful for
    testing / cost-capped re-processing).
    """
    total = {
        "processed": 0,
        "source": 0,
        "rule": 0,
        "source_fallback": 0,
        "llm": 0,
        "deferred": 0,
    }
    for _ in range(max_batches):
        r = await categorize_backlog(
            use_llm=use_llm, llm_cap=llm_cap_per_batch
        )
        for k in total:
            total[k] += r.get(k, 0)
        if r.get("processed", 0) == 0:
            break
    return total


@router.post("/reclassify-rules")
async def trigger_reclassify_rules():
    """Re-apply source-level + keyword rules to every item.

    Safe to call repeatedly — only items whose category would change get
    touched. This is the one-shot "fix historical mislabels" button.
    """
    return await reclassify_all_with_rules()


@router.post("/translate")
async def trigger_translate(llm_cap: int = 30):
    """Translate one batch of untranslated EN knowledge/news items."""
    return await translate_backlog(llm_cap=llm_cap)


@router.post("/translate-drain")
async def trigger_translate_drain(
    max_batches: int = 50, llm_cap_per_batch: int = 30
):
    """Drain the translation backlog (bounded by max_batches)."""
    return await translate_drain(
        max_batches=max_batches, llm_cap_per_batch=llm_cap_per_batch
    )


@router.post("/dedupe-snapshots")
async def trigger_dedupe_snapshots():
    """Remove duplicate ItemSnapshot rows (same item_id + captured_at).

    These duplicates used to cause the same item to render twice on the
    home page. Keeps the row with the highest id per (item_id, captured_at)
    and deletes the rest.
    """
    from sqlalchemy import text

    from ..database import session_factory

    factory = session_factory()
    async with factory() as session:
        # NB: this assumes the schema name is available in search_path for the
        # session, which get_content_collector_db sets up. Using a raw DELETE
        # because SQLAlchemy doesn't express "keep MAX(id) per group" cleanly.
        result = await session.execute(
            text(
                """
                DELETE FROM content_collector.item_snapshots s
                USING (
                    SELECT item_id,
                           captured_at,
                           MAX(id) AS keep_id
                    FROM content_collector.item_snapshots
                    GROUP BY item_id, captured_at
                    HAVING COUNT(*) > 1
                ) d
                WHERE s.item_id = d.item_id
                  AND s.captured_at = d.captured_at
                  AND s.id <> d.keep_id
                """
            )
        )
        deleted = result.rowcount or 0
        await session.commit()

    return {"deleted": deleted}
