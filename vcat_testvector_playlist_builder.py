#!/usr/bin/env python3
import json
from pathlib import Path
from typing import List

import settings as cfg
from vcat_testvector_datamodels import (
    VcatTestVectorPlaylistAsset,
    VcatTestVectorPlaylistManifest,
    VcatTestVectorHeader
)
from utils import getChecksum

def get_video_manifests_local() -> List[Path]:
    """
    Return all *_video_manifest.json files under cfg.MANIFEST_DIR
    """
    return list(cfg.MANIFEST_DIR.glob("*_video_manifest.json"))


def generate_playlist_from_video_manifest(manifest_path: Path):
    """
    Read one video manifest JSON from disk, produce a one‑entry playlist
    manifest, and write it back into the same folder.
    """
    # 1) Load the video manifest JSON
    with manifest_path.open("r") as f:
        video_manifest = json.load(f)

    header = video_manifest["vcat_testvector_header"]
    ma     = video_manifest["media_asset"]

    # 2) Sanity check
    if "video_mime_type" not in ma:
        print(f"  → skipping {manifest_path.name}, not a video manifest")
        return

    # 3) Compute checksum of the video‑manifest file itself
    manifest_checksum = getChecksum(str(manifest_path))

    # 4) Build the playlist asset
    #    URL is just the manifest filename (relative to same dir)
    asset_url = f"../manifests/{manifest_path.name}"
    playlist_asset = VcatTestVectorPlaylistAsset(
        name        = header["name"],
        url         = asset_url,
        checksum    = manifest_checksum,
        length_bytes= ma["length_bytes"],
        uuid        = header["uuid"],
        description = header["description"]
    )

    # 5) Build the playlist header
    playlist_header = VcatTestVectorHeader(
        name        = header["name"] + "_playlist",
        description = f"Playlist for {header['name']}",
        created_by  = header["created_by"]
    )

    # 6) Assemble the playlist manifest object
    playlist_manifest = VcatTestVectorPlaylistManifest(
        vcat_testvector_header = playlist_header,
        media_assets           = [playlist_asset]
    )

    # 7) Write it back out alongside the video manifests
    out_name   = f"{playlist_header.name}.json"
    out_path   = cfg.MANIFEST_DIR / out_name
    with out_path.open("w") as out:
        json.dump(playlist_manifest.to_dict(), out, indent=2)

    print(f"✔ Wrote playlist {out_name} in {cfg.MANIFEST_DIR}")


def main():
    video_manifests = get_video_manifests_local()
    print(f"Found {len(video_manifests)} video manifest(s) in {cfg.MANIFEST_DIR}\n")

    for vm in video_manifests:
        print(f"Generating playlist from {vm.name}…")
        generate_playlist_from_video_manifest(vm)


if __name__ == "__main__":
    main()
