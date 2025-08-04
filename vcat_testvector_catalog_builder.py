#!/usr/bin/env python3
import json
from pathlib import Path
from typing import List

import settings as cfg
from vcat_testvector_datamodels import (
    VcatTestVectorPlaylistAsset,
    VcatTestVectorPlaylistCatalog,
    VcatTestVectorHeader
)
from utils import getChecksum

def find_local_playlists() -> List[Path]:
    """
    Return all *_playlist.json files under cfg.MANIFEST_DIR
    """
    return list(cfg.MANIFEST_DIR.glob("*_playlist.json"))

def build_catalog() -> VcatTestVectorPlaylistCatalog:
    """
    Read every playlist manifest in cfg.MANIFEST_DIR and assemble a Catalog object.
    """
    playlist_files = find_local_playlists()
    assets: List[VcatTestVectorPlaylistAsset] = []

    for p in playlist_files:
        # 1) Load the JSON just to pull out the header fields
        data = json.loads(p.read_text())
        hdr  = data["vcat_testvector_header"]

        # 2) Compute checksum & length of the .json file itself
        checksum     = getChecksum(str(p))
        length_bytes = p.stat().st_size

        # 3) Build the “url” field relative to the catalog’s location.
        #    If your catalog lives at BASE_OUTPUT_DIR, and playlists in MANIFEST_DIR,
        #    you might want something like “manifests/filename.json”
        rel_url = f"manifests/{p.name}"

        asset = VcatTestVectorPlaylistAsset(
            name         = hdr["name"],
            url          = rel_url,
            checksum     = checksum,
            length_bytes = length_bytes,
            uuid         = hdr["uuid"],
            description  = hdr["description"]
        )
        assets.append(asset)

    # 4) Create a fresh catalog header
    catalog_header = VcatTestVectorHeader(
        name        = "VCAT Demo Test Assets",
        description = "Auto‐generated playlist catalog",
        created_by  = "RoncaTech, LLC"
    )

    return VcatTestVectorPlaylistCatalog(
        vcat_testvector_header = catalog_header,
        playlists              = assets
    )

def write_catalog_to_disk(catalog: VcatTestVectorPlaylistCatalog):
    """
    Serialize the catalog to JSON under cfg.CATALOG_OUTPUT_DIR.
    """
    out_path = cfg.BASE_OUTPUT_DIR / "vcat_testvector_playlist_catalog.json"

    with out_path.open("w") as f:
        json.dump(catalog.to_dict(), f, indent=2)

    print(f"▶️  Catalog written to {out_path}")

def main():
    catalog = build_catalog()
    write_catalog_to_disk(catalog)

if __name__ == "__main__":
    main()
