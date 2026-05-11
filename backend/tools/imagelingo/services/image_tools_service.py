"""Image tools service — background removal (GPT Image) and smart resize/crop.

Background removal: uses GPT Image edit to remove background and output transparent/white-bg image.
Smart resize: algorithm-based crop (center) + optional AI extend (GPT Image outpainting).
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import time
import urllib.request
from io import BytesIO
from typing import Literal

from PIL import Image

logger = logging.getLogger(__name__)

# Preset aspect ratios for e-commerce platforms
PRESETS = {
    "1:1": (1, 1),
    "4:3": (4, 3),
    "3:2": (3, 2),
    "3:4": (3, 4),
    "2:3": (2, 3),
    "16:9": (16, 9),
    "9:16": (9, 16),
}


# ── Background Removal (GPT Image) ──────────────────────────────────────

async def remove_background(image_bytes: bytes, output_format: str = "png") -> bytes:
    """Remove background from image using GPT Image edit.
    Returns PNG bytes with transparent background or JPG with white background.
    """
    from backend.tools.imagelingo.services.gpt_image_service import _call_image_edit

    t0 = time.perf_counter()

    # Prepare image: resize to API-compatible size
    img = Image.open(BytesIO(image_bytes))
    if img.mode in ("P",):
        img = img.convert("RGBA")

    orig_size = img.size

    # Resize to max 1024px for API
    max_dim = 1024
    if max(img.size) > max_dim:
        ratio = max_dim / max(img.size)
        new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
        img = img.resize(new_size, Image.LANCZOS)

    # Determine API size
    w, h = img.size
    aspect = w / h
    if aspect > 1.18:
        api_size = "1536x1024"
    elif aspect < 0.85:
        api_size = "1024x1536"
    else:
        api_size = "1024x1024"

    # Convert to JPEG for API input
    if img.mode == "RGBA":
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[3])
        img = bg
    elif img.mode != "RGB":
        img = img.convert("RGB")

    buf = BytesIO()
    img.save(buf, format="JPEG", quality=90)
    prepared_bytes = buf.getvalue()

    prompt = (
        "Remove the background from this image completely. "
        "Keep ONLY the main subject/product with pixel-perfect edges. "
        "The background should be pure white (#FFFFFF). "
        "Do NOT alter the product in any way — preserve all details, colors, shadows, and textures exactly. "
        "The result should look like a professional product photo on a clean white background."
    )

    result_bytes = await _call_image_edit(prepared_bytes, prompt, "high", api_size)

    elapsed = time.perf_counter() - t0
    logger.info("Background removal (GPT Image): %.2fs, input=%d bytes", elapsed, len(image_bytes))

    # Restore to original size if needed
    result_img = Image.open(BytesIO(result_bytes))
    if result_img.size != orig_size:
        result_img = result_img.resize(orig_size, Image.LANCZOS)

    if output_format == "png":
        buf = BytesIO()
        # GPT Image returns white bg, convert to PNG (no true transparency but clean white bg)
        result_img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    else:
        # JPEG output
        if result_img.mode == "RGBA":
            result_img = result_img.convert("RGB")
        elif result_img.mode != "RGB":
            result_img = result_img.convert("RGB")
        buf = BytesIO()
        result_img.save(buf, format="JPEG", quality=92)
        return buf.getvalue()


# ── Smart Resize / Crop ──────────────────────────────────────────────────

def _compute_crop_box(
    src_w: int, src_h: int, target_ratio: float
) -> tuple[int, int, int, int]:
    """Compute center crop box to achieve target aspect ratio.
    Returns (left, top, right, bottom).
    """
    src_ratio = src_w / src_h

    if src_ratio > target_ratio:
        # Image is wider than target — crop sides
        new_w = int(src_h * target_ratio)
        offset = (src_w - new_w) // 2
        return (offset, 0, offset + new_w, src_h)
    else:
        # Image is taller than target — crop top/bottom
        new_h = int(src_w / target_ratio)
        offset = (src_h - new_h) // 2
        return (0, offset, src_w, offset + new_h)


def _compute_pad_box(
    src_w: int, src_h: int, target_ratio: float
) -> tuple[int, int, tuple[int, int]]:
    """Compute padding needed to achieve target aspect ratio.
    Returns (new_width, new_height, (pad_x, pad_y)).
    """
    src_ratio = src_w / src_h

    if src_ratio > target_ratio:
        # Image is wider — add height (pad top/bottom)
        new_h = int(src_w / target_ratio)
        new_w = src_w
        pad_x = 0
        pad_y = (new_h - src_h) // 2
    else:
        # Image is taller — add width (pad left/right)
        new_w = int(src_h * target_ratio)
        new_h = src_h
        pad_x = (new_w - src_w) // 2
        pad_y = 0

    return new_w, new_h, (pad_x, pad_y)


async def smart_resize(
    image_bytes: bytes,
    target_ratio: str,
    mode: Literal["crop", "pad", "ai_extend"] = "crop",
    output_size: int | None = None,
    bg_color: str = "#FFFFFF",
) -> bytes:
    """Resize image to target aspect ratio.

    Modes:
    - crop: center crop to target ratio (default, fast, free)
    - pad: add solid color padding to reach target ratio
    - ai_extend: use GPT Image to outpaint/extend the image (costs credits)

    Args:
        image_bytes: source image bytes
        target_ratio: one of PRESETS keys ("1:1", "4:3", etc.) or "W:H" format
        mode: resize strategy
        output_size: optional max dimension in pixels (e.g. 1024)
        bg_color: hex color for padding mode (default white)
    """
    t0 = time.perf_counter()

    img = Image.open(BytesIO(image_bytes))
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    src_w, src_h = img.size

    # Parse target ratio
    if target_ratio in PRESETS:
        rw, rh = PRESETS[target_ratio]
    else:
        parts = target_ratio.split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid ratio format: {target_ratio}. Use 'W:H' like '4:3'.")
        rw, rh = int(parts[0]), int(parts[1])

    ratio = rw / rh
    current_ratio = src_w / src_h

    # If already close enough (within 2%), just resize
    if abs(current_ratio - ratio) / ratio < 0.02:
        result = img
    elif mode == "crop":
        box = _compute_crop_box(src_w, src_h, ratio)
        result = img.crop(box)
    elif mode == "pad":
        new_w, new_h, (pad_x, pad_y) = _compute_pad_box(src_w, src_h, ratio)
        # Parse bg_color
        color = _parse_hex_color(bg_color)
        canvas = Image.new("RGB", (new_w, new_h), color)
        canvas.paste(img, (pad_x, pad_y))
        result = canvas
    elif mode == "ai_extend":
        # Use GPT Image outpainting — delegate to separate function
        result_bytes = await _ai_extend(image_bytes, img, ratio, output_size)
        logger.info("AI extend: %.2fs", time.perf_counter() - t0)
        return result_bytes
    else:
        raise ValueError(f"Unknown mode: {mode}")

    # Apply output_size constraint
    if output_size:
        w, h = result.size
        if max(w, h) > output_size:
            scale = output_size / max(w, h)
            result = result.resize(
                (int(w * scale), int(h * scale)), Image.LANCZOS
            )

    buf = BytesIO()
    result.save(buf, format="JPEG", quality=92, optimize=True)
    out = buf.getvalue()

    logger.info("Smart resize (%s): %dx%d → %dx%d in %.2fs",
                mode, src_w, src_h, result.size[0], result.size[1], time.perf_counter() - t0)
    return out


def _parse_hex_color(hex_str: str) -> tuple[int, int, int]:
    """Parse hex color string to RGB tuple."""
    hex_str = hex_str.lstrip("#")
    if len(hex_str) == 3:
        hex_str = "".join(c * 2 for c in hex_str)
    if len(hex_str) != 6:
        return (255, 255, 255)
    return (int(hex_str[0:2], 16), int(hex_str[2:4], 16), int(hex_str[4:6], 16))


async def _ai_extend(
    image_bytes: bytes, img: Image.Image, target_ratio: float, output_size: int | None
) -> bytes:
    """Use GPT Image to extend/outpaint image to target ratio.
    This consumes credits and takes ~30-40s.
    """
    from backend.tools.imagelingo.services.gpt_image_service import _call_image_edit

    src_w, src_h = img.size

    # Determine API size based on target ratio
    if target_ratio > 1.18:
        api_size = "1536x1024"  # landscape
    elif target_ratio < 0.85:
        api_size = "1024x1536"  # portrait
    else:
        api_size = "1024x1024"  # square

    # Resize source to fit within API size
    api_w, api_h = map(int, api_size.split("x"))
    scale = min(api_w / src_w, api_h / src_h)
    new_w, new_h = int(src_w * scale), int(src_h * scale)
    resized = img.resize((new_w, new_h), Image.LANCZOS)

    # Place on white canvas
    canvas = Image.new("RGB", (api_w, api_h), (255, 255, 255))
    paste_x = (api_w - new_w) // 2
    paste_y = (api_h - new_h) // 2
    canvas.paste(resized, (paste_x, paste_y))

    buf = BytesIO()
    canvas.save(buf, format="JPEG", quality=90)
    prepared_bytes = buf.getvalue()

    prompt = (
        "Extend this product image naturally to fill the entire canvas. "
        "The white/empty areas around the product should be filled with a clean, "
        "professional background that matches the existing image style. "
        "Keep the product exactly as-is, only extend the background/surroundings."
    )

    result_bytes = await _call_image_edit(prepared_bytes, prompt, "high", api_size)

    # Optionally resize to output_size
    if output_size:
        result_img = Image.open(BytesIO(result_bytes))
        w, h = result_img.size
        if max(w, h) > output_size:
            scale = output_size / max(w, h)
            result_img = result_img.resize(
                (int(w * scale), int(h * scale)), Image.LANCZOS
            )
        buf = BytesIO()
        result_img.save(buf, format="JPEG", quality=92)
        result_bytes = buf.getvalue()

    return result_bytes


# ── Download helper ──────────────────────────────────────────────────────

def download_image(url: str, max_size: int = 10 * 1024 * 1024) -> bytes:
    """Download image from URL with size limit."""
    req = urllib.request.Request(url, headers={"User-Agent": "ImageLingo/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = resp.read(max_size + 1)
    if len(data) > max_size:
        raise ValueError(f"Image too large (>{max_size // 1024 // 1024}MB)")
    return data
