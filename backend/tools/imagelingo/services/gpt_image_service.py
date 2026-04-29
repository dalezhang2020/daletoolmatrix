"""GPT Image Service — Azure OpenAI GPT-image-2 for image translation.
Uses the /images/edits endpoint to translate text in product images.
Synchronous API, no polling needed. Much more reliable than Lovart.
"""
from __future__ import annotations

import base64
import logging
import os
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

    # Determine the base endpoint (strip /v1 suffix if present for Azure REST API)
    endpoint = AZURE_ENDPOINT.rstrip("/")
    if endpoint.endswith("/v1"):
        endpoint = endpoint[:-3]

    # Build the edit URL
    edit_url = (
        f"{endpoint}/openai/deployments/{AZURE_DEPLOYMENT}"
        f"/images/edits?api-version={AZURE_API_VERSION}"
    )

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

    # Upload the base64 image to a public URL
    # For now, use Lovart's upload if available, otherwise return a data URI
    image_result_bytes = base64.b64decode(b64_data)

    # Try to upload to Lovart CDN (reuse existing infrastructure)
    try:
        from backend.tools.imagelingo.services.lovart_service import LovartService
        lovart = LovartService()
        cdn_url = lovart.upload_file(image_result_bytes, f"translated_{target_language}.png")
        logger.info("Translated image uploaded to Lovart CDN: %s", cdn_url)
        return cdn_url
    except Exception as upload_err:
        logger.warning("Lovart upload failed, using inline hosting: %s", upload_err)

    # Fallback: save to a temporary file and serve via a simple endpoint
    # For production, you'd want to upload to S3/R2/etc.
    # For now, store in DB as base64 and return a serving URL
    import uuid
    image_id = str(uuid.uuid4())[:12]

    # Store in a simple file-based cache (Railway has ephemeral storage, but OK for short-lived results)
    cache_dir = "/tmp/imagelingo_results"
    os.makedirs(cache_dir, exist_ok=True)
    file_path = f"{cache_dir}/{image_id}.png"
    with open(file_path, "wb") as f:
        f.write(image_result_bytes)

    # Return a URL that the translate route can serve
    # This requires adding a static file serving route
    logger.info("Translated image saved locally: %s", file_path)
    return f"/api/imagelingo/translate/results/{image_id}.png"
