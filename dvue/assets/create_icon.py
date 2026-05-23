"""
Generate dvue application icons.

Produces:
  icon.png   — 256×256 source PNG
  icon.ico   — Windows multi-resolution ICO (16, 32, 48, 64, 128, 256 px)

Design
------
  Background  : deep ocean-teal rounded square (#0d3349 → #0e5472 gradient)
  Wave        : two-layer time-series waveform — white opacity layers
  Text        : "dv" in a clean sans-serif, lower-right corner

Run
---
  python dvue/assets/create_icon.py
"""

from __future__ import annotations

import math
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
except ImportError as exc:
    raise ImportError(
        "Pillow is required to generate the icon.\n"
        "Install it with:  pip install Pillow"
    ) from exc

OUTPUT_DIR = Path(__file__).parent
SIZE = 256          # base canvas size
CORNER_R = 46       # rounded-corner radius (≈18 % of size)

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
BG_TOP    = (13,  51,  73)   # deep teal-navy
BG_BOTTOM = (14,  84, 114)   # brighter ocean blue
WAVE1     = (255, 255, 255, 90)   # subtle background wave (RGBA)
WAVE2     = (255, 255, 255, 200)  # bright foreground wave (RGBA)
TEXT_CLR  = (255, 255, 255, 220)  # "dv" label

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rounded_mask(size: int, radius: int) -> Image.Image:
    """Return a white-on-black mask with rounded corners."""
    mask = Image.new("L", (size, size), 0)
    d = ImageDraw.Draw(mask)
    d.rounded_rectangle([0, 0, size - 1, size - 1], radius=radius, fill=255)
    return mask


def _vertical_gradient(size: int, top_rgb: tuple, bottom_rgb: tuple) -> Image.Image:
    """Return an RGBA image filled with a top→bottom linear gradient."""
    img = Image.new("RGBA", (size, size))
    for y in range(size):
        t = y / (size - 1)
        r = int(top_rgb[0] + t * (bottom_rgb[0] - top_rgb[0]))
        g = int(top_rgb[1] + t * (bottom_rgb[1] - top_rgb[1]))
        b = int(top_rgb[2] + t * (bottom_rgb[2] - top_rgb[2]))
        for x in range(size):
            img.putpixel((x, y), (r, g, b, 255))
    return img


def _wave_points(
    width: int,
    baseline: float,
    amplitude: float,
    frequency: float,
    phase: float,
    n_points: int = 120,
) -> list[tuple[float, float]]:
    """Return a smooth wave polyline as (x, y) float pairs."""
    pts = []
    for i in range(n_points + 1):
        x = i * width / n_points
        # combine two sine harmonics for a natural-looking irregular wave
        y = baseline - amplitude * (
            0.6 * math.sin(2 * math.pi * frequency * i / n_points + phase)
            + 0.4 * math.sin(2 * math.pi * frequency * 2.5 * i / n_points + phase * 1.3)
        )
        pts.append((x, y))
    return pts


# ---------------------------------------------------------------------------
# Build icon
# ---------------------------------------------------------------------------

def build_icon(size: int = SIZE) -> Image.Image:
    scale = size / SIZE  # scale helper so design works at any resolution

    # ── Background (gradient + rounded mask) ──────────────────────────────
    bg = _vertical_gradient(size, BG_TOP, BG_BOTTOM)
    mask = _rounded_mask(size, radius=int(CORNER_R * scale))

    icon = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    icon.paste(bg, mask=mask)

    # ── Overlay layer for waves ────────────────────────────────────────────
    wave_layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    wd = ImageDraw.Draw(wave_layer)

    # Background subtle wave (filled band below the line)
    w1 = _wave_points(size, baseline=size * 0.58, amplitude=size * 0.13,
                      frequency=2.6, phase=0.4)
    w1_closed = w1 + [(size, size), (0, size)]
    wd.polygon(w1_closed, fill=(255, 255, 255, 18))
    wd.line(w1, fill=(255, 255, 255, 70), width=max(1, int(1.5 * scale)))

    # Foreground brighter wave
    w2 = _wave_points(size, baseline=size * 0.48, amplitude=size * 0.17,
                      frequency=2.2, phase=1.1)
    w2_closed = w2 + [(size, size), (0, size)]
    wd.polygon(w2_closed, fill=(255, 255, 255, 28))
    wd.line(w2, fill=(255, 255, 255, 210), width=max(1, int(2.5 * scale)))

    # Small "tick" dots on the bright wave at regular intervals (data points)
    dot_r = max(2, int(4 * scale))
    step = len(w2) // 7
    for i in range(step, len(w2) - 1, step):
        x, y = w2[i]
        wd.ellipse([x - dot_r, y - dot_r, x + dot_r, y + dot_r],
                   fill=(255, 255, 255, 230))

    icon = Image.alpha_composite(icon, wave_layer)

    # ── "dv" text label ────────────────────────────────────────────────────
    text_layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    td = ImageDraw.Draw(text_layer)

    font_size = int(72 * scale)
    font: ImageFont.ImageFont | None = None
    # Try to load a system font; fall back to default
    for candidate in [
        "arialbd.ttf",       # Windows bold Arial
        "Arial Bold.ttf",
        "DejaVuSans-Bold.ttf",
        "LiberationSans-Bold.ttf",
    ]:
        try:
            font = ImageFont.truetype(candidate, font_size)
            break
        except (OSError, IOError):
            continue
    if font is None:
        font = ImageFont.load_default()

    label = "dv"
    bbox = td.textbbox((0, 0), label, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    tx = size - tw - int(18 * scale) - bbox[0]
    ty = size - th - int(18 * scale) - bbox[1]
    td.text((tx, ty), label, font=font, fill=TEXT_CLR)

    icon = Image.alpha_composite(icon, text_layer)

    # ── Apply rounded mask to final composite ─────────────────────────────
    final = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    final.paste(icon, mask=mask)
    return final


# ---------------------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------------------

def main() -> None:
    print("Building dvue icon …")

    # 256-px PNG (source of truth)
    img256 = build_icon(256)
    png_path = OUTPUT_DIR / "icon.png"
    img256.save(png_path, format="PNG")
    print(f"  Saved: {png_path}")

    # Multi-resolution ICO
    sizes = [16, 32, 48, 64, 128, 256]
    frames = [build_icon(s) for s in sizes]
    ico_path = OUTPUT_DIR / "icon.ico"
    frames[0].save(
        ico_path,
        format="ICO",
        sizes=[(s, s) for s in sizes],
        append_images=frames[1:],
    )
    print(f"  Saved: {ico_path}  ({', '.join(str(s) for s in sizes)} px)")
    print("Done.")


if __name__ == "__main__":
    main()
