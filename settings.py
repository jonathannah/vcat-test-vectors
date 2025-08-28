# settings.py

import os
from pathlib import Path

#
# ── Local FS settings ─────────────────────────────────────────────────────────
#

# 1) Grab the user’s home directory
HOME = Path.home()

# 2) Define your new “base” output folder
BASE_OUTPUT_DIR = HOME / "Downloads" / "roncatech-vcat-test-vectors-vertical"

# 3) Then derive all the sub‐dirs from that
MANIFEST_DIR  = BASE_OUTPUT_DIR / "manifests"
# (or whatever structure you prefer)

# 4) Make sure they exist at startup
# ensure it exists
MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

#
# ── S3 / HTTP settings ────────────────────────────────────────────────────────
#
S3_BUCKET_NAME   = "roncatech-vcat-test-vectors"
S3_REGION        = "us-west-2"
# s3://…
S3_URL           = f"s3://{S3_BUCKET_NAME}"
# https://…amazonaws.com
HTTPS_BASE_URL   = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com"

#
# ── Local FS settings ─────────────────────────────────────────────────────────
#
# Where to write all generated manifests
OUTPUT_MANIFEST_DIR = Path(__file__).parent / "manifests"
# Ensure this directory exists at startup
OUTPUT_MANIFEST_DIR.mkdir(exist_ok=True)

#
# ── Prefixes inside the bucket / local folder ─────────────────────────────────
#
CATALOG_FILENAME    = "vcat_testvector_playlist_catalog.json"
PLAYLISTS_FOLDER    = "manifests"
MEDIA_FOLDER        = "media"

#
# ── Metadata defaults ─────────────────────────────────────────────────────────
#
CREATED_BY          = "RoncaTech, LLC"

# create the dirs on import
for d in (BASE_OUTPUT_DIR, MANIFEST_DIR):
    d.mkdir(parents=True, exist_ok=True)
