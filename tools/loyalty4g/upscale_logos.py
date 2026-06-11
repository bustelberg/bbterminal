"""Ensure every logo PNG is at least MIN x MIN (default 400x400).

The loyalty4g media upload rejects anything smaller:
    "Afbeelding te klein, minimaal vereist formaat is 400 x 400."

Strategy per under-sized image:
  * smaller side >= --letter-below : UPSCALE (preserve aspect ratio,
    smaller side -> MIN) with Lanczos. A <=2.5x upscale of a real logo
    looks fine.
  * smaller side <  --letter-below : the source is a tiny leftover
    favicon (16-128px) that would be a blurry mess upscaled, so instead
    REGENERATE it as a clean 400x400 first-letter badge using the
    company name from the manifest (same badge style the scraper uses
    for Brandfetch-misses). Falls back to upscaling if the name is
    unknown.

Idempotent: images already >= MIN on both sides are left untouched.

Usage:
  uv run --with pillow python tools/loyalty4g/upscale_logos.py --check   # report only
  uv run --with pillow python tools/loyalty4g/upscale_logos.py           # fix in place
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

# ── letter-badge generator (mirrors scrape_leonteq_logos.py) ──────────
_LETTER_BG_PALETTE = [
    (92, 107, 192), (66, 165, 245), (38, 166, 154), (102, 187, 106),
    (236, 64, 122), (171, 71, 188), (126, 87, 194), (255, 167, 38),
    (141, 110, 99), (120, 144, 156), (38, 198, 218), (255, 112, 67),
]
_FONT_CANDIDATES = [
    r"C:\Windows\Fonts\arialbd.ttf",
    r"C:\Windows\Fonts\segoeuib.ttf",
    r"C:\Windows\Fonts\arial.ttf",
    "DejaVuSans-Bold.ttf",
    "DejaVuSans.ttf",
]


def _load_font(size: int):
    for path in _FONT_CANDIDATES:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _letter_badge(name: str, size: int) -> bytes:
    letter = next((c for c in name if c.isalnum()), "?").upper()
    bg = _LETTER_BG_PALETTE[sum(ord(c) for c in name) % len(_LETTER_BG_PALETTE)]
    img = Image.new("RGBA", (size, size), bg + (255,))
    draw = ImageDraw.Draw(img)
    font = _load_font(int(size * 0.55))
    box = draw.textbbox((0, 0), letter, font=font)
    w, h = box[2] - box[0], box[3] - box[1]
    draw.text(
        ((size - w) / 2 - box[0], (size - h) / 2 - box[1]),
        letter, font=font, fill=(255, 255, 255, 255),
    )
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _name_by_filename(manifest_path: str) -> dict[str, str]:
    """{ '<sophisInternalId>.png': name } from the manifest."""
    out: dict[str, str] = {}
    if not Path(manifest_path).is_file():
        return out
    for e in json.loads(Path(manifest_path).read_text(encoding="utf-8")):
        lf = e.get("logo_file")
        if lf:
            out[lf] = (e.get("name") or "").strip()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Ensure logos are >= MIN x MIN.")
    ap.add_argument("--images-dir", default="backend/leonteq_logos/images")
    ap.add_argument("--manifest", default="backend/leonteq_logos/companies.json")
    ap.add_argument("--min-size", type=int, default=400)
    ap.add_argument(
        "--letter-below", type=int, default=160,
        help="Images with smaller side below this are regenerated as letter "
        "badges instead of upscaled (default 160 = ~2.5x cutoff).",
    )
    ap.add_argument("--check", action="store_true", help="Report only; write nothing.")
    args = ap.parse_args()

    d = Path(args.images_dir)
    MIN = args.min_size
    imgs = sorted(d.glob("*.png"))
    if not imgs:
        print(f"No PNGs in {d}", file=sys.stderr)
        return 1
    names = _name_by_filename(args.manifest)

    upscaled = lettered = skipped_ok = 0
    will_upscale = will_letter = 0
    for p in imgs:
        with Image.open(p) as im:
            w, h = im.size
            if w >= MIN and h >= MIN:
                skipped_ok += 1
                continue
            use_letter = min(w, h) < args.letter_below and names.get(p.name)
            if args.check:
                will_letter += 1 if use_letter else 0
                will_upscale += 0 if use_letter else 1
                continue
            if use_letter:
                p.write_bytes(_letter_badge(names[p.name], MIN))
                lettered += 1
            else:
                scale = MIN / min(w, h)
                nw, nh = max(MIN, math.ceil(w * scale)), max(MIN, math.ceil(h * scale))
                im.convert("RGBA").resize((nw, nh), Image.LANCZOS).save(p, format="PNG")
                upscaled += 1
        if (upscaled + lettered) % 50 == 0 and not args.check:
            print(f"  processed {upscaled + lettered}…", flush=True)

    print(f"\ntotal={len(imgs)}  already_ok={skipped_ok}", flush=True)
    if args.check:
        print(f"would upscale: {will_upscale}  would letter-badge: {will_letter}")
        print("(check mode — nothing written)")
    else:
        print(f"upscaled: {upscaled}  letter-badged: {lettered}")
        # Verify nothing remains under MIN.
        remaining = sum(
            1 for p in imgs
            if min(Image.open(p).size) < MIN
        )
        print(f"remaining under {MIN}: {remaining}  (should be 0)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
