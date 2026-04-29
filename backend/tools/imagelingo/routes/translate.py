"""Translation pipeline routes — GPT Image / Lovart → return translated image URL."""
from __future__ import annotations

import logging
import os
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
    "ZH-CN": "Simplified Chinese", "ZH": "Simplified Chinese",
}

# Credit system: each store gets 100 credits/month, each image costs 20 credits
CREDITS_PER_IMAGE = 20
DEFAULT_CREDITS_LIMIT = 100

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


def _get_billing_period(store_id: str) -> str:
    """Return billing period key based on store's installed_at date.
    Each store gets a 30-day rolling cycle starting from install date.
    Period key format: 'YYYY-MM-DD_to_YYYY-MM-DD'."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT installed_at FROM imagelingo.stores WHERE id = %s", (store_id,))
            row = cur.fetchone()
    if not row or not row[0]:
        return datetime.now(timezone.utc).strftime("%Y-%m")  # fallback
    installed_at = row[0]
    now = datetime.now(timezone.utc)
    # Calculate how many full 30-day cycles have passed
    days_since = (now - installed_at).days
    cycle_num = days_since // 30
    cycle_start = installed_at + __import__("datetime").timedelta(days=cycle_num * 30)
    cycle_end = cycle_start + __import__("datetime").timedelta(days=30)
    return f"{cycle_start.strftime('%Y-%m-%d')}_to_{cycle_end.strftime('%Y-%m-%d')}"


def _increment_usage(store_id: str):
    period = _get_billing_period(store_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO imagelingo.usage_logs (store_id, month, credits_used)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (store_id, month) DO UPDATE
                     SET credits_used = imagelingo.usage_logs.credits_used + %s,
                         updated_at = NOW()""",
                (store_id, period, CREDITS_PER_IMAGE, CREDITS_PER_IMAGE),
            )
        conn.commit()


def _check_quota(store_id: str) -> tuple[bool, int, int]:
    """Returns (allowed, credits_used, credits_limit)."""
    period = _get_billing_period(store_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT COALESCE(ul.credits_used, 0), COALESCE(s.credits_limit, %s)
                   FROM imagelingo.subscriptions s
                   LEFT JOIN imagelingo.usage_logs ul ON ul.store_id = s.store_id AND ul.month = %s
                   WHERE s.store_id = %s""",
                (DEFAULT_CREDITS_LIMIT, period, store_id),
            )
            row = cur.fetchone()
    if not row:
        return True, 0, DEFAULT_CREDITS_LIMIT
    used, limit = row
    if limit <= 0:
        return True, used, 0  # unlimited
    return used + CREDITS_PER_IMAGE <= limit, used, limit


# ── Pipeline ─────────────────────────────────────────────────────────────

async def _run_pipeline(job_id: str, store_id: str, image_url: str, target_languages: list[str]):
    _update_job_status(job_id, "processing")
    try:
        # Primary: Azure GPT Image 2 (synchronous, reliable)
        # Fallback: Lovart (async polling, less reliable)
        use_gpt = bool(os.environ.get("AZURE_OPENAI_API_KEY"))

        if use_gpt:
            from backend.tools.imagelingo.services.gpt_image_service import translate_image as gpt_translate
            for lang in target_languages:
                lang_name = LANG_NAMES.get(lang.upper(), lang)
                output_url = await gpt_translate(image_url, lang_name)
                _save_translated_image(job_id, lang, output_url)
        else:
            from backend.tools.imagelingo.services.lovart_service import LovartService
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
    # Require store_handle — never return cross-store data
    if not store_handle:
        raise HTTPException(400, "store_handle is required")
    where = "WHERE j.store_id = (SELECT id FROM imagelingo.stores WHERE handle = %s)"
    params: list = [store_handle]
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
    if store_handle:
        store_clause = "WHERE s.handle = %s"
        params: list = [store_handle]
    else:
        store_clause = ""
        params = []
    # Resolve store to get billing period
    store_id = None
    if store_handle:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM imagelingo.stores WHERE handle = %s", (store_handle,))
                row = cur.fetchone()
                if row:
                    store_id = str(row[0])
    period = _get_billing_period(store_id) if store_id else datetime.now(timezone.utc).strftime("%Y-%m")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT s.id, sub.plan, COALESCE(sub.credits_limit, {DEFAULT_CREDITS_LIMIT}),
                           COALESCE(ul.credits_used, 0)
                    FROM imagelingo.stores s
                    LEFT JOIN imagelingo.subscriptions sub ON sub.store_id = s.id
                    LEFT JOIN imagelingo.usage_logs ul ON ul.store_id = s.id AND ul.month = %s
                    {store_clause} LIMIT 1""",
                [period] + params,
            )
            row = cur.fetchone()
    if not row:
        return {"plan": "free", "credits_limit": DEFAULT_CREDITS_LIMIT, "credits_used": 0,
                "credits_per_image": CREDITS_PER_IMAGE, "period": period}
    _, plan, limit, used = row
    return {"plan": plan or "free", "credits_limit": limit, "credits_used": used,
            "credits_per_image": CREDITS_PER_IMAGE, "period": period}


# ── Image upload (for drag-and-drop local files) ─────────────────────────────

from fastapi import UploadFile, File

@router.post("/upload")
async def upload_image(file: UploadFile = File(...)):
    """Accept a local image file, standardize size, upload to S3, return HTTPS URL."""
    import datetime
    from io import BytesIO

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(400, "File too large (max 10MB)")

    # Standardize image: resize if too large (max 1500px on longest side)
    try:
        from PIL import Image
        img = Image.open(BytesIO(content))
        max_dim = 1500
        if max(img.size) > max_dim:
            ratio = max_dim / max(img.size)
            new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
            img = img.resize(new_size, Image.LANCZOS)
            logger.info("Resized image from %s to %s", img.size, new_size)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85, optimize=True)
        content = buf.getvalue()
    except Exception as e:
        logger.warning("Image standardization failed, using original: %s", e)

    # Upload to S3 via shared utility
    from backend.shared.s3_utils import sign_s3_upload, generate_presigned_url
    import httpx

    cfg = {
        "access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "bucket": os.environ.get("S3_BUCKET", ""),
        "region": os.environ.get("S3_REGION", "us-east-2"),
    }
    if not cfg["access_key"] or not cfg["bucket"]:
        raise HTTPException(500, "S3 not configured (missing AWS_ACCESS_KEY_ID or S3_BUCKET)")

    ext = (file.filename or "upload.jpg").rsplit(".", 1)[-1].lower() if "." in (file.filename or "") else "jpg"
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = str(uuid.uuid4())[:8]
    s3_key = f"imagelingo/uploads/{ts}_{uid}.{ext}"

    signed = sign_s3_upload(
        file_bytes=content,
        bucket=cfg["bucket"],
        object_key=s3_key,
        region=cfg["region"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        content_type=file.content_type or "image/jpeg",
        date=datetime.datetime.now(datetime.timezone.utc),
    )

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.put(signed["url"], headers=signed["headers"], content=content)

    if resp.status_code not in (200, 201):
        raise HTTPException(502, f"S3 upload failed (HTTP {resp.status_code})")

    # Return presigned URL (private bucket, 24h expiry)
    presigned = generate_presigned_url(
        bucket=cfg["bucket"],
        object_key=s3_key,
        region=cfg["region"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        expires_in=86400,
    )
    return {"url": presigned, "key": s3_key}

# ── Serve locally-cached translated images (GPT Image fallback) ──────────────

from fastapi.responses import FileResponse

@router.get("/results/{filename}")
async def serve_result_image(filename: str):
    """Serve a translated image from the local cache (used when CDN upload fails)."""
    import re
    # Sanitize filename to prevent path traversal
    if not re.match(r'^[a-f0-9]{12}\.png$', filename):
        raise HTTPException(400, "Invalid filename")
    file_path = f"/tmp/imagelingo_results/{filename}"
    if not os.path.exists(file_path):
        raise HTTPException(404, "Image not found")
    return FileResponse(file_path, media_type="image/png")
