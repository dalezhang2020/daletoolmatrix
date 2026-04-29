"""GPT Image Service — OCR-assisted image translation.
Pipeline: GPT-4o-mini vision (OCR + translate) → GPT Image edit (render with precise translation table).
The OCR step gives GPT Image exact text-to-translation mappings, eliminating garbled characters.
"""
from __future__ import annotations

import asyncio
import base64
import datetime
import json
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
AZURE_IMAGE_DEPLOYMENT = os.environ.get("AZURE_OPENAI_IMAGE_DEPLOYMENT", "gpt-image-1.5")

_client = None


def _get_client():
    global _client
    if _client is None:
        from openai import OpenAI
        _client = OpenAI(base_url=AZURE_ENDPOINT, api_key=AZURE_API_KEY, max_retries=3)
    return _client


# GPT-image-1.5 supported sizes and accepted aspect ratios
API_SIZES = {
    "square": (1024, 1024),     # 1:1
    "landscape": (1536, 1024),  # 3:2
    "portrait": (1024, 1536),   # 2:3
}

# Accepted aspect ratio ranges for e-commerce images
# ratio = width / height
RATIO_RANGES = {
    "square":    (0.85, 1.18),   # ~1:1 (allows slight deviation)
    "landscape": (1.18, 1.65),   # ~4:3 to ~3:2
    "portrait":  (0.60, 0.85),   # ~2:3 to ~3:4
}


def _classify_aspect(w: int, h: int) -> tuple[str, tuple[int, int]] | None:
    """Classify image into a supported aspect ratio category.
    Returns (category, api_size) or None if unsupported.
    """
    ratio = w / h
    for category, (lo, hi) in RATIO_RANGES.items():
        if lo <= ratio <= hi:
            return category, API_SIZES[category]
    return None


def _download_and_prepare(url: str) -> tuple[bytes, tuple[int, int], tuple[int, int]]:
    """Download image, validate aspect ratio, resize to API size.
    No padding — only resize (stretch-free, content-preserving).
    Raises ValueError if aspect ratio is not supported.
    """
    from PIL import Image

    t0 = time.perf_counter()
    req = urllib.request.Request(url, headers={"User-Agent": "ImageLingo/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()

    img = Image.open(BytesIO(raw))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    orig_w, orig_h = img.size

    # Validate aspect ratio
    result = _classify_aspect(orig_w, orig_h)
    if result is None:
        ratio = orig_w / orig_h
        raise ValueError(
            f"Unsupported image aspect ratio ({orig_w}×{orig_h}, ratio {ratio:.2f}). "
            f"ImageLingo supports e-commerce image ratios: "
            f"1:1 (square), 4:3/3:2 (landscape), 2:3/3:4 (portrait). "
            f"Please crop your image to a supported ratio."
        )

    category, (api_w, api_h) = result

    # Direct resize to API size (no padding needed since ratio is compatible)
    img = img.resize((api_w, api_h), Image.LANCZOS)

    buf = BytesIO()
    img.save(buf, format="JPEG", quality=90, optimize=True)
    out = buf.getvalue()

    logger.info("Prepare %.2fs: %dx%d → %dx%d (%s), %d bytes",
                time.perf_counter() - t0, orig_w, orig_h, api_w, api_h, category, len(out))

    return out, (orig_w, orig_h), (api_w, api_h)


def _restore_original_size(result_bytes: bytes, orig_size: tuple[int, int], api_size: tuple[int, int]) -> bytes:
    """Resize result back to original dimensions. Uses JPEG for speed."""
    from PIL import Image

    if orig_size == api_size:
        return result_bytes

    t0 = time.perf_counter()
    img = Image.open(BytesIO(result_bytes))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    img = img.resize(orig_size, Image.BILINEAR)  # BILINEAR faster than LANCZOS for upscale

    buf = BytesIO()
    img.save(buf, format="JPEG", quality=92, optimize=False)  # no optimize = faster
    out = buf.getvalue()
    logger.info("Restored: %dx%d → %dx%d in %.2fs (%d bytes)",
                api_size[0], api_size[1], orig_size[0], orig_size[1], time.perf_counter() - t0, len(out))
    return out


# ── Step 1: OCR + Translate via GPT-4o vision ───────────────────────────

async def _ocr_and_translate(image_bytes: bytes, target_language: str) -> list[dict]:
    """Use GPT-4o vision to read all text with style info and produce exact translations.
    Returns list of {"original", "translated", "style"} dicts.
    """
    client = _get_client()
    b64 = base64.b64encode(image_bytes).decode()

    from PIL import Image
    img = Image.open(BytesIO(image_bytes))
    w, h = img.size

    prompt = f"""List every piece of visible text in this product image and translate each to {target_language}.

Return JSON: {{"translations": [{{"original": "exact text", "translated": "{target_language} translation"}}]}}

Rules:
- Include ALL text (headings, labels, captions, bullet points)
- Brand names → keep original, set translated = original
- Translations must be accurate and natural"""

    response = await asyncio.to_thread(
        client.chat.completions.create,
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "high"}},
        ]}],
        response_format={"type": "json_object"},
        max_tokens=1500,
    )

    text = response.choices[0].message.content or "{}"
    try:
        data = json.loads(text)
        pairs = data.get("translations") or data.get("text") or []
        if isinstance(data, list):
            pairs = data
    except json.JSONDecodeError:
        logger.error("OCR parse failed: %s", text[:200])
        pairs = []

    logger.info("OCR (GPT-4o) found %d text pairs for %s", len(pairs), target_language)
    for p in pairs:
        logger.info("  '%s' → '%s' [%s]", p.get("original",""), p.get("translated",""), p.get("style",""))
    return pairs


# ── Step 2: Build precise prompt from translation table ──────────────────

def _build_prompt(target_language: str, translation_pairs: list[dict]) -> str:
    """Build a GPT Image edit prompt with exact text-to-translation mappings and style hints."""
    if not translation_pairs:
        return (
            f"Translate ALL visible text in this image into {target_language}. "
            "Keep the exact same layout, design, colors, and positions. "
            "Only replace text, do not change anything else."
        )

    # Build detailed translation table with style info
    table_lines = []
    for i, pair in enumerate(translation_pairs, 1):
        orig = pair.get("original", "").strip()
        trans = pair.get("translated", "").strip()
        style = pair.get("style", "")
        if orig and trans:
            if orig == trans:
                table_lines.append(f'  {i}. "{orig}" → KEEP AS IS (brand name)')
            else:
                style_hint = f" [{style}]" if style else ""
                table_lines.append(f'  {i}. "{orig}" → "{trans}"{style_hint}')

    table = "\n".join(table_lines)

    return (
        f"You are a professional product image localizer. Replace ONLY the text in this image.\n\n"
        f"EXACT TRANSLATION TABLE (use these translations character-by-character, do NOT modify them):\n"
        f"{table}\n\n"
        f"MANDATORY RULES — violating any rule means failure:\n"
        f"1. PIXEL-PERFECT PRESERVATION: The image layout, product photos, graphics, backgrounds, colors, shadows, and all non-text elements must be IDENTICAL to the original. Not similar — identical.\n"
        f"2. CHARACTER ACCURACY: Copy each translated character EXACTLY from the table above. For CJK characters (Chinese/Japanese/Korean), every stroke must be correct. If unsure about a character, use the exact Unicode character from the table.\n"
        f"3. FONT STYLE MATCHING: Match the original text's font weight (bold/regular), size, color, letter-spacing, and visual style as closely as possible.\n"
        f"4. POSITION PRESERVATION: Each translated text must occupy the same position and area as the original text it replaces.\n"
        f"5. NO ADDITIONS: Do not add any text, watermarks, or elements that are not in the original image.\n"
        f"6. NO REMOVALS: Do not remove any non-text visual elements.\n"
        f"7. BRAND NAMES: Items marked 'KEEP AS IS' must remain in their original language.\n\n"
        f"This is a surgical text replacement operation. The output image should be indistinguishable from the original except for the translated text."
    )


# ── Step 3: GPT Image edit with precise prompt ──────────────────────────

async def _call_image_edit(image_bytes: bytes, prompt: str, quality: str, size: str) -> bytes:
    """Call Azure GPT Image edit REST API, return result image bytes."""
    import httpx

    endpoint = AZURE_ENDPOINT.rstrip("/")
    for suffix in ("/openai/v1", "/v1"):
        if endpoint.endswith(suffix):
            endpoint = endpoint[: -len(suffix)]
            break

    edit_url = f"{endpoint}/openai/deployments/{AZURE_IMAGE_DEPLOYMENT}/images/edits?api-version=2025-04-01-preview"

    t0 = time.perf_counter()
    max_retries = 3
    resp = None

    async with httpx.AsyncClient(timeout=180) as http_client:
        for attempt in range(max_retries):
            resp = await http_client.post(
                edit_url,
                files={"image": ("source.jpg", image_bytes, "image/jpeg")},
                data={
                    "prompt": prompt,
                    "n": "1",
                    "size": size,
                    "quality": quality,
                    "input_fidelity": "high",
                },
                headers={"api-key": AZURE_API_KEY},
            )
            if resp.status_code == 200:
                break
            if resp.status_code in (429, 500, 502, 503) and attempt < max_retries - 1:
                wait = (attempt + 1) * 10
                logger.warning("Azure %d on attempt %d, retrying in %ds", resp.status_code, attempt + 1, wait)
                await asyncio.sleep(wait)
                continue
            break

    elapsed = time.perf_counter() - t0
    logger.info("Image edit %.1fs (status=%d, attempts=%d)", elapsed, resp.status_code if resp else 0, attempt + 1)

    if not resp or resp.status_code != 200:
        detail = resp.text[:300] if resp else "No response"
        raise ValueError(f"GPT Image edit failed ({resp.status_code if resp else 0}): {detail}")

    result = resp.json()
    b64_data = result.get("data", [{}])[0].get("b64_json")
    if not b64_data:
        raise ValueError("GPT Image edit returned no image data")

    return base64.b64decode(b64_data)


# ── S3 upload ────────────────────────────────────────────────────────────

async def _upload_to_s3(image_bytes: bytes, target_language: str) -> str | None:
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
    s3_key = f"imagelingo/translated/{ts}_{uid}_{target_language}.jpg"

    signed = sign_s3_upload(
        file_bytes=image_bytes, bucket=cfg["bucket"], object_key=s3_key,
        region=cfg["region"], access_key=cfg["access_key"],
        secret_key=cfg["secret_key"], content_type="image/jpeg",
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


# ── Main entry point ────────────────────────────────────────────────────

async def translate_image(
    image_url: str,
    target_language: str,
    quality: str = "high",
    size: str = "1024x1024",
) -> tuple[bytes, str]:
    """OCR-assisted image translation pipeline.
    Returns (result_bytes, temp_local_url) immediately after AI processing.
    S3 upload is handled separately by the caller.
    """
    if not AZURE_API_KEY:
        raise ValueError("AZURE_OPENAI_API_KEY is not set")

    t_total = time.perf_counter()
    azure_quality = "high"  # always high

    # Step 1: Download + validate aspect ratio
    image_bytes, orig_size, api_size = _download_and_prepare(image_url)
    api_size_str = f"{api_size[0]}x{api_size[1]}"

    # Step 2: OCR + Translate (~3-5s)
    t_ocr = time.perf_counter()
    pairs = await _ocr_and_translate(image_bytes, target_language)
    logger.info("OCR+translate: %.1fs, %d pairs", time.perf_counter() - t_ocr, len(pairs))

    # Step 3: Build precise prompt
    prompt = _build_prompt(target_language, pairs)

    # Step 4: GPT Image edit (~35-45s)
    result_bytes = await _call_image_edit(image_bytes, prompt, azure_quality, api_size_str)

    # Step 5: Restore original dimensions (run in thread - CPU bound)
    if orig_size != api_size:
        result_bytes = await asyncio.to_thread(_restore_original_size, result_bytes, orig_size, api_size)

    logger.info("AI pipeline done: %.1fs (orig=%dx%d)", time.perf_counter() - t_total, orig_size[0], orig_size[1])

    # Save to temp file for immediate serving
    image_id = str(uuid.uuid4())[:12]
    os.makedirs("/tmp/imagelingo_results", exist_ok=True)
    path = f"/tmp/imagelingo_results/{image_id}.png"
    with open(path, "wb") as f:
        f.write(result_bytes)
    temp_url = f"/api/imagelingo/translate/results/{image_id}.png"

    return result_bytes, temp_url
