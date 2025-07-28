import json
import os
from copy import deepcopy

import boto3
from vcat_testvector_datamodels import (VcatTestVectorPlaylistAsset,
                                        VcatTestVectorPlaylistManifest,
                                        VcatTestVectorHeader)
from utils import getTempCopyFromS3, getChecksum
from typing import List


# Initialize the S3 client
s3_client = boto3.client('s3')

# Bucket URL and directory where video manifests are stored
bucket_url = "s3://roncatech-vcat-test-vectors"
manifests_dir = "manifests"


def get_video_manifests(bucket_url: str) -> List[str]:
    """
    Scan the S3 bucket for all video manifest files.
    """
    # Parse the bucket URL
    if not bucket_url.startswith("s3://"):
        raise ValueError("The URL must start with 's3://'")

    parts = bucket_url[5:].split("/", 1)
    bucket_name = parts[0]
    prefix = f"{manifests_dir}/"

    # List objects in the bucket (filtering for manifest files)
    paginator = s3_client.get_paginator('list_objects_v2')
    video_manifests = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                file_name = obj["Key"]
                if file_name.endswith("_video_manifest.json"):  # Look for video manifests
                    video_manifests.append(file_name)

    return video_manifests


def generate_playlist_from_video_manifest(manifest_file: str, bucket_url: str):
    """
    Generates a playlist manifest based on a video manifest.
    """
    # Extract the bucket and file path from the URL
    bucket_name = bucket_url[5:].split("/")[0]  # Extract bucket name
    file_path = manifest_file  # Full path to the video manifest in the 'manifests' folder

    # Log the file path to ensure correctness
    print(f"Fetching manifest from S3: Bucket: {bucket_name}, Key: {file_path}")

    try:
        # Get the video manifest from S3
        manifest_obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        video_manifest = json.loads(manifest_obj['Body'].read())
    except s3_client.exceptions.NoSuchKey as e:
        print(f"Error: The file '{file_path}' was not found in the bucket '{bucket_name}'. Please verify the path.")
        return

    # Extract necessary details from the video manifest
    media_asset = video_manifest['media_asset']

    # Check if it's a valid video manifest
    if 'video_mime_type' not in media_asset:
        print(f"Skipping {manifest_file}, not a valid video manifest")
        return

    video_name = video_manifest["vcat_testvector_header"]["name"]
    video_description = video_manifest["vcat_testvector_header"]["description"]
    video_uuid = video_manifest["vcat_testvector_header"]["uuid"]
    video_mime_type = media_asset["video_mime_type"]
    resolution_x_y = media_asset["resolution_x_y"]

    # Create playlist description (replace "VCAT sample test vector" with "VCAT sample playlist")
    playlist_description = "Playlist video: "+ video_description

    # Create playlist item using VcatTestVectorPlaylistAsset
    # Now we calculate the checksum of the video manifest itself
    # First, download the manifest from S3 to a temporary local file
    temp_file = getTempCopyFromS3(f"{bucket_url}/{file_path}")
    video_manifest_checksum = getChecksum(temp_file)  # Calculate checksum for the video manifest

    # The URL for the playlist asset is simply the URL from S3 used to fetch the manifest
    media_asset_url = f"https://{bucket_name}.s3.amazonaws.com/{file_path}"

    # Create the playlist asset
    media_asset = VcatTestVectorPlaylistAsset(
        name=video_name,
        url=media_asset_url,  # URL points to the video manifest (not video file)
        checksum=video_manifest_checksum,  # Use the checksum of the video manifest
        length_bytes=media_asset["length_bytes"],
        uuid=video_uuid,
        description=video_description
    )

    # Create the playlist manifest using the VcatTestVectorPlaylistManifest
    playlist_manifest = VcatTestVectorPlaylistManifest(
        vcat_testvector_header=VcatTestVectorHeader(
            name=video_name,
            description=playlist_description,
            created_by="RoncaTech, LLC"
        ),
        media_assets=[media_asset]
    )

    # Ensure the './manifests' directory exists, create it if necessary
    output_dir = './manifests'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Clean the filename by removing the path, keeping only the filename
    clean_video_file_name = video_name.split('/')[-1]  # Strip path and keep filename

    # Save the playlist manifest directly inside the './manifests' directory (no subfolders)
    output_file = os.path.join(output_dir, f"{clean_video_file_name}_playlist.json")

    # Write the JSON to the file
    with open(output_file, 'w') as json_file:
        json.dump(playlist_manifest.to_dict(), json_file, indent=4)

    print(f"Playlist for {video_name} saved as {output_file} with uuid = {playlist_manifest.vcat_testvector_header.uuid}")


def main():
    video_manifests = get_video_manifests(bucket_url)

    print(f"Found {len(video_manifests)} video manifests.")

    # Generate a playlist for each video manifest
    for manifest_file in video_manifests:
        print(f"Calling generate_playlist_from_video_manifest for: {manifest_file}")
        generate_playlist_from_video_manifest(manifest_file, bucket_url)


if __name__ == "__main__":
    main()
