"""GPT Image Service — Azure OpenAI GPT-image-2 for image translation.
Uses the /images/edits endpoint to translate text in product images.
Synchronous API, no polling needed. Much more reliable than Lovart.
"""
from __future__ import annotations

import base64
import logging
import os
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

PROMPT_TEMPLATE = (
    "Translate ALL visible text in this product image into {target_lang}. "
    "Keep the EXACT same layout, background, colors, fonts, and visual design. "
    "Replace every piece of text with its accurate {target_lang} translation. "
    "Preserve all non-text elements (logos, icons, product photos) unchanged. "
    "Output the final translated image."
)


def _download_image(url: str) -> bytes:
    """Download image from URL, return bytes."""
    req = urllib.request.Request(url, headers={"User-Agent": "ImageLingo/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


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
) -> str:
    """Translate text in an image using Azure OpenAI GPT-image-2 edit API.

    Returns: URL of the translated image (uploaded to a temporary hosting).
    Since Azure returns base64, we save it and return a data URI or upload it.
    """
    import httpx

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
    async with httpx.AsyncClient(timeout=120) as client:
        files = {
            "image": ("source.png", image_bytes, "image/png"),
        }
        data = {
            "prompt": prompt,
            "n": "1",
            "size": "1024x1024",
            "quality": "high",
        }
        headers = {
            "api-key": api_key,
        }

        resp = await client.post(edit_url, files=files, data=data, headers=headers)

    if resp.status_code != 200:
        error_detail = resp.text[:300]
        logger.error("GPT Image edit failed (%d): %s", resp.status_code, error_detail)
        raise ValueError(f"GPT Image edit failed ({resp.status_code}): {error_detail}")

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

        async with _httpx.AsyncClient(timeout=30) as s3_client:
            s3_resp = await s3_client.put(signed["url"], headers=signed["headers"], content=image_result_bytes)

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
