"""GPT Image Service — Azure OpenAI via OpenAI SDK.
Uses images.edit() for translating text in product images.
SDK handles auth, retries (429/5xx), and response parsing.
"""
from __future__ import annotations

import base64
import datetime
import logging
import os
import time
import uuid
import urllib.request
from io import BytesIO

logger = logging.getLogger(__name__)

AZURE_ENDPOINT = os.environ.get(
    "AZURE_OPENAI_ENDPOINT",
    "https://foundry-llm-zg.services.ai.azure.com/openai/v1",
)
AZURE_API_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")
AZURE_DEPLOYMENT = os.environ.get("AZURE_OPENAI_IMAGE_DEPLOYMENT", "gpt-image-1.5")
SOURCE_MAX_DIM = int(os.environ.get("IMAGELINGO_SOURCE_MAX_DIM", "1024"))

PROMPT_TEMPLATE = (
    "ONLY replace the text in this image — do NOT change anything else. "
    "Translate all visible text into {target_lang}. "
    "\n\nSTRICT RULES:\n"
    "1. The image layout, product photos, graphics, colors, and positions must remain PIXEL-PERFECT identical to the original.\n"
    "2. ONLY modify the text regions — replace English text with {target_lang} translation.\n"
    "3. Keep the same font style, size, weight, and color for each text element.\n"
    "4. Brand names (like product names, logos) must stay in the original language.\n"
    "5. Every translated character must be correct and clearly readable — no garbled or wrong characters.\n"
    "6. For Chinese: use standard Simplified Chinese (简体中文) only.\n"
    "7. Do NOT move, resize, crop, or reposition any image element.\n"
    "\nThis is a text replacement task, NOT an image regeneration task."
)

_client = None


def _get_client():
    global _client
    if _client is None:
        from openai import OpenAI
        _client = OpenAI(
            base_url=AZURE_ENDPOINT,
            api_key=AZURE_API_KEY,
            max_retries=3,  # SDK auto-retries 429/5xx
        )
    return _client


def _download_and_resize(url: str) -> bytes:
    """Download image, resize to max SOURCE_MAX_DIM px, return PNG bytes."""
    t0 = time.perf_counter()
    req = urllib.request.Request(url, headers={"User-Agent": "ImageLingo/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
    try:
        from PIL import Image
        img = Image.open(BytesIO(raw))
        if max(img.size) > SOURCE_MAX_DIM:
            ratio = SOURCE_MAX_DIM / max(img.size)
            img = img.resize((int(img.size[0] * ratio), int(img.size[1] * ratio)), Image.LANCZOS)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        buf = BytesIO()
        img.save(buf, format="PNG", optimize=True)
        out = buf.getvalue()
        logger.info("Download+resize %.2fs (%d→%d bytes)", time.perf_counter() - t0, len(raw), len(out))
        return out
    except Exception:
        return raw


async def _upload_to_s3(image_bytes: bytes, target_language: str) -> str | None:
    """Upload translated image to S3, return presigned URL."""
    cfg = {
        "access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "bucket": os.environ.get("S3_BUCKET", ""),
        "region": os.environ.get("S3_REGION", "us-east-1"),
    }
    if not cfg["access_key"] or not cfg["bucket"]:
        return None

    from backend.shared.s3_utils import sign_s3_upload, generate_presigned_url
    import httpx

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = str(uuid.uuid4())[:8]
    s3_key = f"imagelingo/translated/{ts}_{uid}_{target_language}.png"

    signed = sign_s3_upload(
        file_bytes=image_bytes, bucket=cfg["bucket"], object_key=s3_key,
        region=cfg["region"], access_key=cfg["access_key"],
        secret_key=cfg["secret_key"], content_type="image/png",
        date=datetime.datetime.now(datetime.timezone.utc),
    )
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.put(signed["url"], headers=signed["headers"], content=image_bytes)
    if resp.status_code not in (200, 201):
        return None

    return generate_presigned_url(
        bucket=cfg["bucket"], object_key=s3_key, region=cfg["region"],
        access_key=cfg["access_key"], secret_key=cfg["secret_key"], expires_in=86400,
    )


async def translate_image(
    image_url: str,
    target_language: str,
    quality: str = "medium",
    size: str = "1024x1024",
) -> str:
    """Translate text in an image using OpenAI SDK images.edit()."""
    if not AZURE_API_KEY:
        raise ValueError("AZURE_OPENAI_API_KEY is not set")

    azure_quality = {"fast": "medium", "low": "medium", "medium": "medium", "high": "high"}.get(quality, "medium")
    prompt = PROMPT_TEMPLATE.format(target_lang=target_language)

    # Step 1: Download and resize
    image_bytes = _download_and_resize(image_url)

    # Step 2: Call Azure REST API /images/edits with input_fidelity=high
    # (SDK doesn't support input_fidelity, so we use REST directly)
    import httpx

    endpoint = AZURE_ENDPOINT.rstrip("/")
    for suffix in ("/openai/v1", "/v1"):
        if endpoint.endswith(suffix):
            endpoint = endpoint[: -len(suffix)]
            break

    edit_url = f"{endpoint}/openai/deployments/{AZURE_DEPLOYMENT}/images/edits?api-version=2025-04-01-preview"

    t0 = time.perf_counter()
    logger.info("images/edits REST: model=%s, target=%s, quality=%s, fidelity=high, %d bytes",
                AZURE_DEPLOYMENT, target_language, azure_quality, len(image_bytes))

    max_retries = 3
    resp = None
    async with httpx.AsyncClient(timeout=180) as http_client:
        for attempt in range(max_retries):
            resp = await http_client.post(
                edit_url,
                files={"image": ("source.png", image_bytes, "image/png")},
                data={
                    "prompt": prompt,
                    "n": "1",
                    "size": size,
                    "quality": azure_quality,
                    "input_fidelity": "high",
                },
                headers={"api-key": AZURE_API_KEY},
            )
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 500, 502, 503) and attempt < max_retries - 1:
                import asyncio as _aio
                wait = (attempt + 1) * 10
                logger.warning("Azure %d on attempt %d, retrying in %ds", resp.status_code, attempt + 1, wait)
                await _aio.sleep(wait)
                continue
            break

    elapsed = time.perf_counter() - t0
    logger.info("images/edits completed in %.1fs (status=%d, attempts=%d)", elapsed, resp.status_code if resp else 0, attempt + 1)

    if not resp or resp.status_code != 200:
        detail = resp.text[:300] if resp else "No response"
        raise ValueError(f"GPT Image edit failed ({resp.status_code if resp else 0}): {detail}")

    result = resp.json()

    # Step 3: Decode result
    b64_data = result.get("data", [{}])[0].get("b64_json")
    if not b64_data:
        raise ValueError("GPT Image edit returned no image data")
    image_result = base64.b64decode(b64_data)

    # Step 4: Upload to S3
    s3_url = await _upload_to_s3(image_result, target_language)
    if s3_url:
        return s3_url

    # Fallback: local file
    image_id = str(uuid.uuid4())[:12]
    os.makedirs("/tmp/imagelingo_results", exist_ok=True)
    path = f"/tmp/imagelingo_results/{image_id}.png"
    with open(path, "wb") as f:
        f.write(image_result)
    return f"/api/imagelingo/translate/results/{image_id}.png"
