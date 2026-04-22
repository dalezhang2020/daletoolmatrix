"""Translation pipeline routes — OCR → Lovart (translate+render) → return image URL."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from backend.db.connection import get_connection
from backend.tools.imagelingo.services.token_store import get_token

logger = logging.getLogger(__name__)

LANG_NAMES = {
    "EN": "English", "EN-US": "English", "EN-GB": "English",
    "DE": "German", "JA": "Japanese", "KO": "Korean",
    "FR": "French", "ES": "Spanish",
}

router = APIRouter()


# ── Request / Response models ────────────────────────────────────────────

class TranslateRequest(BaseModel):
    store_handle: str
    product_id: str
    image_url: str
    target_languages: list[str]


class TranslateResponse(BaseModel):
    job_id: str


class BatchTranslateRequest(BaseModel):
    store_handle: str
    product_id: str
    image_urls: list[str]
    target_languages: list[str]


class BatchTranslateResponse(BaseModel):
    job_ids: list[str]


# ── DB helpers ───────────────────────────────────────────────────────────

def _resolve_store(handle: str) -> tuple[str, str]:
    if not handle:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, handle FROM imagelingo.stores LIMIT 1")
                row = cur.fetchone()
        if row:
            return str(row[0]), row[1]
        raise HTTPException(404, "No store found")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM imagelingo.stores WHERE handle = %s", (handle,))
            row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Store not found")
    return str(row[0]), handle


def _create_job(store_id: str, product_id: str, image_url: str, langs: list[str]) -> str:
    job_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO imagelingo.translation_jobs
                   (id, store_id, product_id, original_image_url, target_languages, status)
                   VALUES (%s, %s, %s, %s, %s, 'pending')""",
                (job_id, store_id, product_id, image_url, langs),
            )
        conn.commit()
    return job_id


def _update_job_status(job_id: str, status: str, error_msg: str = None):
    completed_at = datetime.now(timezone.utc) if status in ("done", "failed") else None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE imagelingo.translation_jobs SET status=%s, error_msg=%s, completed_at=%s WHERE id=%s",
                (status, error_msg, completed_at, job_id),
            )
        conn.commit()


def _save_translated_image(job_id: str, language: str, output_url: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO imagelingo.translated_images (job_id, language, output_url) VALUES (%s, %s, %s)",
                (job_id, language, output_url),
            )
        conn.commit()


def _increment_usage(store_id: str):
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO imagelingo.usage_logs (store_id, month, images_translated)
                   VALUES (%s, %s, 1)
                   ON CONFLICT (store_id, month) DO UPDATE
                     SET images_translated = imagelingo.usage_logs.images_translated + 1,
                         updated_at = NOW()""",
                (store_id, month),
            )
        conn.commit()


def _check_quota(store_id: str) -> tuple[bool, int, int]:
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COALESCE(ul.images_translated, 0), COALESCE(s.images_limit, 5)
                   FROM imagelingo.subscriptions s
                   LEFT JOIN imagelingo.usage_logs ul ON ul.store_id = s.store_id AND ul.month = %s
                   WHERE s.store_id = %s""",
                (month, store_id),
            )
            row = cur.fetchone()
    if not row:
        return True, 0, 5
    used, limit = row
    if limit <= 0:
        return True, used, 0
    return used < limit, used, limit


# ── Pipeline ─────────────────────────────────────────────────────────────

async def _run_pipeline(job_id: str, store_id: str, image_url: str, target_languages: list[str]):
    from backend.tools.imagelingo.services.lovart_service import LovartService

    _update_job_status(job_id, "processing")
    try:
        lovart = LovartService()
        for lang in target_languages:
            lang_name = LANG_NAMES.get(lang.upper(), lang)
            output_url = await lovart.translate_image(image_url, lang_name)
            _save_translated_image(job_id, lang, output_url)
        _update_job_status(job_id, "done")
        _increment_usage(store_id)
    except Exception as exc:
        _update_job_status(job_id, "failed", str(exc))


# ── Routes ───────────────────────────────────────────────────────────────

@router.post("/", response_model=TranslateResponse)
async def start_translation(req: TranslateRequest, background_tasks: BackgroundTasks):
    store_id, handle = _resolve_store(req.store_handle.strip())
    token = get_token(handle)
    if not token:
        raise HTTPException(401, "Store not authenticated or token expired")
    ok, used, limit = _check_quota(store_id)
    if not ok:
        raise HTTPException(402, f"Monthly quota exceeded ({used}/{limit}). Please upgrade your plan.")
    job_id = _create_job(store_id, req.product_id, req.image_url, req.target_languages)
    background_tasks.add_task(_run_pipeline, job_id, store_id, req.image_url, req.target_languages)
    return TranslateResponse(job_id=job_id)


@router.post("/batch", response_model=BatchTranslateResponse)
async def start_batch_translation(req: BatchTranslateRequest, background_tasks: BackgroundTasks):
    if not req.image_urls:
        raise HTTPException(400, "image_urls must not be empty")
    store_id, handle = _resolve_store(req.store_handle.strip())
    token = get_token(handle)
    if not token:
        raise HTTPException(401, "Store not authenticated or token expired")
    ok, used, limit = _check_quota(store_id)
    if not ok:
        raise HTTPException(402, f"Monthly quota exceeded ({used}/{limit}). Please upgrade your plan.")
    job_ids: list[str] = []
    for image_url in req.image_urls:
        job_id = _create_job(store_id, req.product_id, image_url, req.target_languages)
        background_tasks.add_task(_run_pipeline, job_id, store_id, image_url, req.target_languages)
        job_ids.append(job_id)
    return BatchTranslateResponse(job_ids=job_ids)


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, error_msg, original_image_url, target_languages, created_at FROM imagelingo.translation_jobs WHERE id = %s",
                (job_id,),
            )
            job = cur.fetchone()
            if not job:
                raise HTTPException(404, "Job not found")
            status, error_msg, original_url, langs, created_at = job
            cur.execute("SELECT language, output_url FROM imagelingo.translated_images WHERE job_id = %s", (job_id,))
            rows = cur.fetchall()
    return {
        "job_id": job_id,
        "status": status,
        "original_image_url": original_url,
        "target_languages": langs,
        "created_at": created_at.isoformat() if created_at else None,
        "results": {lang: url for lang, url in rows},
        "error": error_msg,
    }


@router.post("/jobs/{job_id}/retry", response_model=TranslateResponse)
async def retry_job(job_id: str, background_tasks: BackgroundTasks):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT store_id, original_image_url, target_languages, status FROM imagelingo.translation_jobs WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Job not found")
    store_id, image_url, langs, status = row
    if status not in ("failed",):
        raise HTTPException(400, "Only failed jobs can be retried")
    store_id = str(store_id)
    _update_job_status(job_id, "pending")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM imagelingo.translated_images WHERE job_id = %s", (job_id,))
        conn.commit()
    background_tasks.add_task(_run_pipeline, job_id, store_id, image_url, langs)
    return TranslateResponse(job_id=job_id)


@router.get("/history")
async def get_history(store_handle: str = ""):
    where = ""
    params: list = []
    if store_handle:
        where = "WHERE j.store_id = (SELECT id FROM imagelingo.stores WHERE handle = %s)"
        params.append(store_handle)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT j.id, j.original_image_url, j.target_languages, j.status,
                           j.created_at, j.error_msg
                    FROM imagelingo.translation_jobs j {where}
                    ORDER BY j.created_at DESC LIMIT 100""",
                params,
            )
            jobs = cur.fetchall()
            job_ids = [str(r[0]) for r in jobs]
            results_map: dict[str, dict[str, str]] = {jid: {} for jid in job_ids}
            if job_ids:
                cur.execute(
                    "SELECT job_id, language, output_url FROM imagelingo.translated_images WHERE job_id = ANY(%s::uuid[])",
                    (job_ids,),
                )
                for jid, lang, url in cur.fetchall():
                    results_map[str(jid)][lang] = url
    return [
        {
            "id": str(j[0]),
            "original_image_url": j[1],
            "target_languages": j[2],
            "status": j[3],
            "created_at": j[4].isoformat() if j[4] else None,
            "error": j[5],
            "results": results_map.get(str(j[0]), {}),
        }
        for j in jobs
    ]


@router.get("/usage")
async def get_usage(store_handle: str = ""):
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    if store_handle:
        store_clause = "WHERE s.handle = %s"
        params: list = [store_handle]
    else:
        store_clause = ""
        params = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT s.id, sub.plan, COALESCE(sub.images_limit, 5),
                           COALESCE(ul.images_translated, 0)
                    FROM imagelingo.stores s
                    LEFT JOIN imagelingo.subscriptions sub ON sub.store_id = s.id
                    LEFT JOIN imagelingo.usage_logs ul ON ul.store_id = s.id AND ul.month = %s
                    {store_clause} LIMIT 1""",
                [month] + params,
            )
            row = cur.fetchone()
    if not row:
        return {"plan": "free", "limit": 5, "used": 0, "month": month}
    _, plan, limit, used = row
    return {"plan": plan or "free", "limit": limit or 5, "used": used, "month": month}
