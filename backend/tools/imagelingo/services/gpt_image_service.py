"""GPT Image Service — Azure OpenAI GPT-image-2 for image translation.
Uses the /images/edits endpoint to translate text in product images.
Synchronous API, no polling needed. Much more reliable than Lovart.
"""
from __future__ import annotations

import base64
import logging
import os
import time
import uuid
import urllib.request

logger = logging.getLogger(__name__)

# Azure OpenAI config
AZURE_ENDPOINT = os.environ.get(
    "AZURE_OPENAI_ENDPOINT",
    "https://foundry-llm-zg.services.ai.azure.com/openai/v1",
)
AZURE_API_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")
AZURE_DEPLOYMENT = os.environ.get("AZURE_OPENAI_IMAGE_DEPLOYMENT", "gpt-image-2-1")
AZURE_API_VERSION = "2025-04-01-preview"
AZURE_IMAGE_QUALITY = os.environ.get("AZURE_OPENAI_IMAGE_QUALITY", "medium")
SOURCE_MAX_DIM = int(os.environ.get("IMAGELINGO_SOURCE_MAX_DIM", "1024"))

PROMPT_TEMPLATE = (
    "Translate ALL visible text in this product image into {target_lang}. "
    "Keep the EXACT same layout, background, colors, fonts, and visual design. "
    "Replace every piece of text with its accurate {target_lang} translation. "
    "Preserve all non-text elements (logos, icons, product photos) unchanged. "
    "Output the final translated image."
)


def _download_image(url: str) -> bytes:
    """Download image from URL, return bytes. Resize to max 1024px for faster processing."""
    t0 = time.perf_counter()
    req = urllib.request.Request(url, headers={"User-Agent": "ImageLingo/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()

    # Resize to 1024px max — matches GPT Image output size, speeds up API call
    try:
        from PIL import Image
        from io import BytesIO
        img = Image.open(BytesIO(raw))
        max_dim = SOURCE_MAX_DIM
        if max(img.size) > max_dim:
            ratio = max_dim / max(img.size)
            new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
            img = img.resize(new_size, Image.LANCZOS)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        buf = BytesIO()
        img.save(buf, format="PNG", optimize=True)
        out = buf.getvalue()
        logger.info("Source download+resize completed in %.2fs (%d -> %d bytes, max_dim=%d)",
                    time.perf_counter() - t0, len(raw), len(out), max_dim)
        return out
    except Exception:
        logger.info("Source download completed in %.2fs (%d bytes, resize skipped)",
                    time.perf_counter() - t0, len(raw))
        return raw


def _build_edit_url(raw_endpoint: str) -> str:
    """Normalize AZURE_OPENAI_ENDPOINT to Azure image-edits REST URL."""
    endpoint = raw_endpoint.strip().rstrip("/")
    if endpoint.endswith("/openai/v1"):
        endpoint = endpoint[: -len("/openai/v1")]
    elif endpoint.endswith("/v1"):
        endpoint = endpoint[: -len("/v1")]
    if not endpoint.endswith("/openai"):
        endpoint = f"{endpoint}/openai"
    return (
        f"{endpoint}/deployments/{AZURE_DEPLOYMENT}"
        f"/images/edits?api-version={AZURE_API_VERSION}"
    )


async def translate_image(
    image_url: str,
    target_language: str,
    quality: str = "medium",
    size: str = "1024x1024",
) -> str:
    """Translate text in an image using Azure OpenAI GPT-image-2 edit API."""
    import httpx

    # Map frontend quality names to Azure API values
    azure_quality = {"fast": "low", "low": "low", "medium": "medium", "high": "high"}.get(quality, "medium")

    api_key = AZURE_API_KEY
    if not api_key:
        raise ValueError("AZURE_OPENAI_API_KEY is not set")

    # Download the source image
    image_bytes = _download_image(image_url)

    # Build Azure image-edits URL from endpoint variants:
    # https://xxx.azure.com, https://xxx.azure.com/openai, or https://xxx.azure.com/openai/v1
    edit_url = _build_edit_url(AZURE_ENDPOINT)
    logger.info("GPT Image edit URL: %s", edit_url)

    prompt = PROMPT_TEMPLATE.format(target_lang=target_language)

    # Azure image edit API uses multipart/form-data
    logger.info("Calling GPT Image edit: deployment=%s, target=%s, image_size=%d bytes",
                AZURE_DEPLOYMENT, target_language, len(image_bytes))

    t_azure = time.perf_counter()
    max_retries = 3
    resp = None

    async with httpx.AsyncClient(timeout=180) as client:
        files_data = ("source.png", image_bytes, "image/png")
        headers = {"api-key": api_key}

        for attempt in range(max_retries):
            # httpx consumes file objects, rebuild each attempt
            resp = await client.post(
                edit_url,
                files={"image": files_data},
                data={"prompt": prompt, "n": "1", "size": size, "quality": azure_quality},
                headers=headers,
            )

            if resp.status_code == 200:
                break

            # Retry on 429 (rate limit) and 5xx (server errors)
            if resp.status_code in (429, 500, 502, 503) and attempt < max_retries - 1:
                wait = (attempt + 1) * 10  # 10s, 20s
                logger.warning("GPT Image %d on attempt %d, retrying in %ds...",
                               resp.status_code, attempt + 1, wait)
                import asyncio
                await asyncio.sleep(wait)
                continue

            break

    elapsed_azure = time.perf_counter() - t_azure
    logger.info("GPT Image request completed in %.2fs (status=%d, quality=%s, size=%s, attempts=%d)",
                elapsed_azure, resp.status_code if resp else 0, quality, size, attempt + 1)

    if not resp or resp.status_code != 200:
        error_detail = resp.text[:300] if resp else "No response"
        logger.error("GPT Image edit failed (%d): %s", resp.status_code if resp else 0, error_detail)
        raise ValueError(f"GPT Image edit failed ({resp.status_code if resp else 0}): {error_detail}")

    result = resp.json()
    b64_data = result.get("data", [{}])[0].get("b64_json")
    if not b64_data:
        raise ValueError("GPT Image edit returned no image data")

    # Upload the translated image to S3
    image_result_bytes = base64.b64decode(b64_data)

    import datetime
    s3_cfg = {
        "access_key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "bucket": os.environ.get("S3_BUCKET", ""),
        "region": os.environ.get("S3_REGION", "us-east-2"),
    }

    if s3_cfg["access_key"] and s3_cfg["bucket"]:
        from backend.shared.s3_utils import sign_s3_upload, generate_presigned_url
        import httpx as _httpx

        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        uid = str(uuid.uuid4())[:8]
        s3_key = f"imagelingo/translated/{ts}_{uid}_{target_language}.png"

        signed = sign_s3_upload(
            file_bytes=image_result_bytes,
            bucket=s3_cfg["bucket"],
            object_key=s3_key,
            region=s3_cfg["region"],
            access_key=s3_cfg["access_key"],
            secret_key=s3_cfg["secret_key"],
            content_type="image/png",
            date=datetime.datetime.now(datetime.timezone.utc),
        )

        t_s3 = time.perf_counter()
        async with _httpx.AsyncClient(timeout=30) as s3_client:
            s3_resp = await s3_client.put(signed["url"], headers=signed["headers"], content=image_result_bytes)
        logger.info("Translated image S3 upload completed in %.2fs (status=%d)",
                    time.perf_counter() - t_s3, s3_resp.status_code)

        if s3_resp.status_code in (200, 201):
            # Return presigned URL (private bucket, 24h expiry)
            presigned = generate_presigned_url(
                bucket=s3_cfg["bucket"],
                object_key=s3_key,
                region=s3_cfg["region"],
                access_key=s3_cfg["access_key"],
                secret_key=s3_cfg["secret_key"],
                expires_in=86400,
            )
            logger.info("Translated image uploaded to S3: %s", s3_key)
            return presigned

        logger.warning("S3 upload failed (%d), falling back to local", s3_resp.status_code)

    # Fallback: save locally
    image_id = str(uuid.uuid4())[:12]
    cache_dir = "/tmp/imagelingo_results"
    os.makedirs(cache_dir, exist_ok=True)
    file_path = f"{cache_dir}/{image_id}.png"
    with open(file_path, "wb") as f:
        f.write(image_result_bytes)
    logger.info("Translated image saved locally: %s", file_path)
    return f"/api/imagelingo/translate/results/{image_id}.png"
