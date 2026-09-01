#!/usr/bin/env python3
"""
Computes one L2-normalized CLIP centroid embedding per model for slopvault's
Discover panel. Runs 100% locally — reads thumbnails/images from disk and
never sends any image or embedding anywhere. The only network access is the
one-time CLIP pretrained-weight download on first run (see requirements.txt).

Incremental: a model is only (re)embedded if its raw media file count
(images+gif+webm) has changed since the last run — that count is stored
alongside each vector as `sourceFileCount`. On a night where nothing was
scraped, every model is skipped via a cheap directory listing and torch is
never even imported. This is what makes it cheap enough to run frequently
against the full dataset instead of needing a separate "backfill vs.
incremental" mode.

Usage:
  python compute_embeddings.py --dataset-dir <path> --thumb-dir <path>

Writes (atomically): <thumb-dir>/embeddings.json
  { "<username>": { "vector": [floats...], "sampleCount": int,
                     "sourceFileCount": int, "computedAt": iso8601 } }
Models with zero available media are omitted from the output entirely.
Models no longer present under --dataset-dir are dropped from the output.

Note: an earlier version of this script also did CLIP zero-shot body-type
categorization (thick/curvy vs bbw vs ssbbw etc.) for the Discover panel's
grouped-browsing sections. That was dropped after testing showed it doesn't
work: the category text prompts embed at 0.82-0.92 cosine similarity to
each other regardless of wording, model size (tried ViT-B-32 through
ViT-L-14), or classification method (centroid vs. per-photo averaging) —
CLIP's text encoder just doesn't carve out a usable "body size" axis for
this content. The `vector` this script produces is still used for
similarity-based recommendations ("you might like"), which is a different,
image-to-image comparison that doesn't hit the same problem.
"""
import argparse
import glob
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

MIN_THUMBS_BEFORE_FALLBACK = 5
MAX_SAMPLES_PER_MODEL = 60
BATCH_SIZE = 32
MODEL_NAME = 'ViT-B-32'
PRETRAINED = 'laion2b_s34b_b79k'
RAW_MEDIA_FOLDERS = ('images', 'gif', 'webm')


def log(msg):
    print(msg, flush=True)


def load_existing(out_path):
    try:
        with open(out_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def count_source_files(dataset_dir, username):
    total = 0
    for folder in RAW_MEDIA_FOLDERS:
        try:
            total += len(os.listdir(os.path.join(dataset_dir, username, folder)))
        except OSError:
            pass
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dataset-dir', required=True)
    ap.add_argument('--thumb-dir', required=True)
    ap.add_argument(
        '--force',
        action='store_true',
        help='Re-embed every model regardless of sourceFileCount (e.g. after '
        'changing MODEL_NAME/PRETRAINED, or for the first-ever backfill run).',
    )
    args = ap.parse_args()
    t0 = time.time()

    out_path = os.path.join(args.thumb_dir, 'embeddings.json')
    existing = load_existing(out_path)

    try:
        model_dirs = sorted(
            d
            for d in os.listdir(args.dataset_dir)
            if os.path.isdir(os.path.join(args.dataset_dir, d)) and not d.startswith('.')
        )
    except OSError as e:
        log(f'FATAL: cannot read dataset dir: {e}')
        sys.exit(1)

    log(f'Found {len(model_dirs)} models')

    current_counts = {u: count_source_files(args.dataset_dir, u) for u in model_dirs}
    to_embed = [
        u
        for u in model_dirs
        if args.force
        or u not in existing
        or existing[u].get('sourceFileCount') != current_counts[u]
    ]
    reused = [u for u in model_dirs if u not in to_embed]

    log(
        f'{len(to_embed)} model(s) new/changed, {len(reused)} unchanged '
        f'(reusing cached embeddings)'
    )

    results = {u: existing[u] for u in reused if u in existing}
    skipped = []

    if to_embed:
        try:
            import torch
            import open_clip
            from PIL import Image  # noqa: F401 (imported here to fail fast together)
        except ImportError as e:
            log(f'FATAL: missing dependency ({e}). Run: pip install -r requirements.txt')
            sys.exit(1)

        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        log(f'Device: {device}')
        model, _, preprocess = open_clip.create_model_and_transforms(
            MODEL_NAME, pretrained=PRETRAINED
        )
        model = model.to(device).eval()
        if device == 'cuda':
            try:
                model.half()
            except Exception:
                pass

        for username in to_embed:
            paths = _collect_sample_paths(args.dataset_dir, args.thumb_dir, username)
            if not paths:
                skipped.append(username)
                continue
            vec, device = _embed_images(paths, model, preprocess, device, torch)
            if vec is None:
                skipped.append(username)
                continue
            results[username] = {
                'vector': vec,
                'sampleCount': len(paths),
                'sourceFileCount': current_counts[username],
                'computedAt': datetime.now(timezone.utc).isoformat(),
            }
            log(f'  {username}: {len(paths)} samples')

    tmp_path = out_path + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(results, f)
    os.replace(tmp_path, out_path)  # atomic swap — a crash mid-write never corrupts what discover.js reads

    log(
        f'Done: {len(results)} models total ({len(to_embed) - len(skipped)} newly '
        f'embedded, {len(reused)} reused, {len(skipped)} skipped/no media), '
        f'{time.time() - t0:.1f}s'
    )
    if skipped:
        shown = ', '.join(skipped[:20]) + (' …' if len(skipped) > 20 else '')
        log(f'Skipped (no media found): {shown}')


def _collect_sample_paths(dataset_dir, thumb_dir, username):
    thumbs = glob.glob(os.path.join(thumb_dir, username, 'thumb-*.jpg'))
    if len(thumbs) >= MIN_THUMBS_BEFORE_FALLBACK:
        random.shuffle(thumbs)
        return thumbs[:MAX_SAMPLES_PER_MODEL]

    # Model has too few (or zero) cached thumbnails — likely never opened in
    # the dashboard and the nightly grid-thumb prewarm hasn't reached it yet.
    # Fall back to sampling raw images/gif files directly so it isn't
    # permanently excluded from recommendations until the next nightly pass.
    raw = []
    for folder in ('images', 'gif'):
        raw.extend(glob.glob(os.path.join(dataset_dir, username, folder, '*')))
    pool = thumbs + raw
    if not pool:
        return []
    random.shuffle(pool)
    return pool[:MAX_SAMPLES_PER_MODEL]


def _embed_images(paths, model, preprocess, device, torch):
    from PIL import Image
    import numpy as np

    vecs = []
    batch = []

    def flush_batch():
        if not batch:
            return
        with torch.no_grad():
            t = torch.stack(batch).to(device)
            if device == 'cuda':
                t = t.half()
            out = model.encode_image(t).float()
            out = out / out.norm(dim=-1, keepdim=True)
            vecs.extend(out.cpu().numpy().tolist())
        batch.clear()

    for p in paths:
        try:
            img = Image.open(p).convert('RGB')
            batch.append(preprocess(img))
        except Exception as e:
            log(f'    warn: skipping unreadable image {p}: {e}')
            continue
        if len(batch) >= BATCH_SIZE:
            try:
                flush_batch()
            except RuntimeError as e:
                # e.g. "no kernel image is available" on a torch build that
                # doesn't support this GPU's compute capability — fall back
                # to CPU for the rest of the run instead of aborting.
                if device == 'cuda':
                    log(f'    warn: CUDA inference failed ({e}); falling back to CPU')
                    device = 'cpu'
                    model.float().to(device)
                    flush_batch()
                else:
                    raise

    flush_batch()
    if not vecs:
        return None, device

    centroid = np.mean(np.array(vecs), axis=0)
    norm = np.linalg.norm(centroid)
    if norm > 0:
        centroid = centroid / norm
    return centroid.tolist(), device


if __name__ == '__main__':
    main()
