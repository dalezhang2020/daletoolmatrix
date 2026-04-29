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
    "Translate ALL visible text in this product image into {target_lang}. "
    "Keep the EXACT same layout, background, colors, fonts, and visual design. "
    "Replace every piece of text with its accurate {target_lang} translation. "
    "Preserve all non-text elements (logos, icons, product photos) unchanged. "
    "Output the final translated image."
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

    azure_quality = {"fast": "low", "low": "low", "medium": "medium", "high": "high"}.get(quality, "medium")
    prompt = PROMPT_TEMPLATE.format(target_lang=target_language)

    # Step 1: Download and resize
    image_bytes = _download_and_resize(image_url)

    # Step 2: Call SDK images.edit() (auto-retries 429/5xx)
    client = _get_client()
    t0 = time.perf_counter()
    logger.info("images.edit: model=%s, target=%s, quality=%s, size=%s, %d bytes",
                AZURE_DEPLOYMENT, target_language, azure_quality, size, len(image_bytes))

    import asyncio
    result = await asyncio.to_thread(
        client.images.edit,
        model=AZURE_DEPLOYMENT,
        image=("source.png", image_bytes, "image/png"),
        prompt=prompt,
        n=1,
        size=size,
        quality=azure_quality,
    )

    elapsed = time.perf_counter() - t0
    logger.info("images.edit completed in %.1fs", elapsed)

    # Step 3: Decode result
    b64_data = result.data[0].b64_json
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
