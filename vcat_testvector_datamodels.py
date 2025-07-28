from dataclasses import dataclass, field
from typing import List, Optional
import uuid
from datetime import datetime

@dataclass
class VcatTestVectorHeader:
    name: str
    description: str
    created_by: str
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))  # Automatically generate UUID
    created_at: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def to_dict(self) -> dict:
        """Converts the VcatTestVectorHeader instance to a dictionary."""
        return {
            "name": self.name,
            "uuid": self.uuid,
            "description": self.description,
            "created_at": self.created_at,
            "created_by": self.created_by
        }


@dataclass
class VcatTestVectorAsset:
    name: str
    url: str
    checksum: str
    length_bytes: str

    def to_dict(self) -> dict:
        """Converts the VcatTestVectorAsset instance to a dictionary."""
        return {
            "name": self.name,
            "url": self.url,
            "checksum": self.checksum,
            "length_bytes": self.length_bytes
        }



@dataclass
class VcatTestVectorVideoAsset(VcatTestVectorAsset):
    video_mime_type: str
    duration_ms: Optional[int]
    resolution_x_y: str
    frame_rate: str

    def to_dict(self) -> dict:
        """Converts the VcatTestVectorVideoAsset instance to a dictionary."""
        asset_dict = super().to_dict()
        asset_dict.update({
            "video_mime_type": self.video_mime_type,
            "duration_ms": self.duration_ms,
            "resolution_x_y": self.resolution_x_y,
            "frame_rate": self.frame_rate
        })
        return asset_dict

@dataclass
class VcatTestVectorVideoManifest:
    vcat_testvector_header: VcatTestVectorHeader
    media_asset: VcatTestVectorVideoAsset

    def to_dict(self) -> dict:
        """Converts the VcatTestVectorVideoManifest instance to a dictionary."""
        return {
            "vcat_testvector_header": self.vcat_testvector_header.to_dict(),
            "media_asset": self.media_asset.to_dict()
        }



# VcatTestVectorPlaylistAsset that extends VcatTestVectorAsset
@dataclass
class VcatTestVectorPlaylistAsset(VcatTestVectorAsset):
    uuid: str
    description: str  # Now a required field with no default value

    def to_dict(self) -> dict:
        """Converts the VcatTestVectorPlaylistAsset instance to a dictionary."""
        asset_dict = super().to_dict()  # Call the parent's to_dict method
        asset_dict.update({
            "uuid": self.uuid,
            "description": self.description,
        })
        return asset_dict

@dataclass
class VcatTestVectorPlaylistManifest:
    vcat_testvector_header: VcatTestVectorHeader
    media_assets: List[VcatTestVectorPlaylistAsset]  # List of VcatTestVectorPlaylistAsset objects

    def to_dict(self) -> dict:
        """Converts the VcatTestVectorPlaylistManifest instance to a dictionary."""
        return {
            "vcat_testvector_header": self.vcat_testvector_header.to_dict(),
            "media_assets": [asset.to_dict() for asset in self.media_assets]
        }


@dataclass
class VcatTestVectorPlaylistCatalog:
    vcat_testvector_header: VcatTestVectorHeader
    playlists: List[VcatTestVectorPlaylistAsset]  # List of VcatTestVectorPlaylistAsset objects

    def to_dict(self) -> dict:
        """Converts the VcatTestVectorPlaylistManifest instance to a dictionary."""
        return {
            "vcat_testvector_header": self.vcat_testvector_header.to_dict(),
            "playlists": [asset.to_dict() for asset in self.playlists]
        }



