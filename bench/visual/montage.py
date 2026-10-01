#!/usr/bin/env python3
"""Side-by-side montages (main | PR | Loki) and a pixel-diff score main vs PR.

  montage.py OUT

Reads OUT/shots/<page>/<range>/{main,pr,loki}.png, writes
OUT/montage/<page>-<range>.png (downscaled, palette PNG) and OUT/pixeldiff.json
(fraction of pixels whose colour differs between main and PR by more than a
small threshold; 0.0 = identical).
"""
import glob
import os
import sys

from PIL import Image, ImageChops, ImageDraw


from vio import dump_json  # noqa: E402

W = 640  # width of each panel in the montage


def load_image(path):
    with Image.open(path) as im:
        return im.convert("RGB")


def label(img, text):
    bar = Image.new("RGB", (img.width, 22), (30, 30, 30))
    ImageDraw.Draw(bar).text((6, 5), text, fill=(255, 255, 255))
    out = Image.new("RGB", (img.width, img.height + 22))
    out.paste(bar, (0, 0))
    out.paste(img, (0, 22))
    return out


MAX_BYTES = 300_000  # a montage hosted on the pr-visuals branch stays at or below this


def save_small(img, path, limit=MAX_BYTES):
    """Palette PNG; fewer colours, then a smaller image, until the file fits `limit`."""
    for scale in (1.0, 0.85, 0.7, 0.55):
        im = img if scale == 1.0 else img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)
        for colors in (128, 64, 32):
            im.quantize(colors=colors, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE).save(path, optimize=True)
            if os.path.getsize(path) <= limit:
                return


def score(a, b):
    if a.size != b.size:
        return 1.0
    diff = ImageChops.difference(a, b).convert("L").point(lambda v: 255 if v > 24 else 0)
    return round(sum(1 for v in diff.tobytes() if v) / (a.width * a.height), 5)


def main():
    out = sys.argv[1]
    os.makedirs(os.path.join(out, "montage"), exist_ok=True)
    scores = {}
    for d in sorted(glob.glob(os.path.join(out, "shots", "*", "*"))):
        page, rng = d.split(os.sep)[-2:]
        imgs = {n: load_image(os.path.join(d, f"{n}.png")) for n in ("main", "pr", "loki") if os.path.exists(os.path.join(d, f"{n}.png"))}
        if "main" not in imgs or "pr" not in imgs:
            continue
        if "loki" not in imgs:  # not captured for this range (more history than Loki holds)
            imgs["loki"] = Image.new("RGB", imgs["main"].size, (245, 245, 245))
            ImageDraw.Draw(imgs["loki"]).text((20, 20), "Loki: not captured for this range (it holds 1.5h of data)", fill=(60, 60, 60))
        scores[f"{page}-{rng}"] = score(imgs["main"], imgs["pr"])
        h = int(imgs["main"].height * W / imgs["main"].width)
        tiles = [label(imgs[n].resize((W, h), Image.LANCZOS), t) for n, t in
                 (("main", "main (before)"), ("pr", "PR (after)"), ("loki", "Loki (reference)"))]
        m = Image.new("RGB", (W * 3 + 8, tiles[0].height), (255, 255, 255))
        for i, t in enumerate(tiles):
            m.paste(t, (i * (W + 4), 0))
        save_small(m, os.path.join(out, "montage", f"{page}-{rng}.png"))
    dump_json(os.path.join(out, "pixeldiff.json"), scores)
    for k, v in scores.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
