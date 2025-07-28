import json
import os
import boto3
from vcat_testvector_datamodels import (VcatTestVectorPlaylistAsset,
                                        VcatTestVectorPlaylistCatalog,
                                        VcatTestVectorHeader)
from utils import getChecksum, getTempCopyFromS3  # Assuming these exist already
from typing import List


# Initialize the S3 client
s3_client = boto3.client('s3')

# Bucket URL and directory where manifests are stored
bucket_url = "s3://roncatech-vcat-test-vectors"
manifests_dir = "manifests"


def get_all_manifests_from_s3(bucket_url: str) -> List[str]:
    """
    Get all manifest files from the S3 bucket.
    """
    # Parse the bucket URL
    if not bucket_url.startswith("s3://"):
        raise ValueError("The URL must start with 's3://'")

    parts = bucket_url[5:].split("/", 1)
    bucket_name = parts[0]
    prefix = f"{manifests_dir}/"

    # List objects in the bucket (filtering for manifest files)
    paginator = s3_client.get_paginator('list_objects_v2')
    all_manifests = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                file_name = obj["Key"]
                all_manifests.append(file_name)

    return all_manifests


def process_playlist_manifest(manifest_file: str, bucket_url: str):
    """
    Processes a playlist manifest to add to the library.
    """
    # Extract the bucket and file path from the URL
    bucket_name = bucket_url[5:].split("/")[0]  # Extract bucket name
    file_path = manifest_file  # Full path to the playlist manifest in the 'manifests' folder

    print(f"Fetching playlist manifest from S3: Bucket: {bucket_name}, Key: {file_path}")

    try:
        # Get the playlist manifest from S3
        manifest_obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        playlist_manifest = json.loads(manifest_obj['Body'].read())
    except s3_client.exceptions.NoSuchKey as e:
        print(f"Error: The file '{file_path}' was not found in the bucket '{bucket_name}'. Please verify the path.")
        return

    # Check if it's a valid playlist manifest
    if 'media_assets' not in playlist_manifest:
        print(f"Skipping {manifest_file}, not a valid playlist manifest")
        return

    # Extract necessary details from the playlist manifest
    playlist_header = playlist_manifest["vcat_testvector_header"]
    playlist_uuid =playlist_header["uuid"]
    playlist_name = playlist_header["name"]
    playlist_description = playlist_header["description"]

    # Download the playlist manifest to calculate checksum and get length
    temp_file = getTempCopyFromS3(f"{bucket_url}/{file_path}")
    playlist_checksum = getChecksum(temp_file)  # Calculate checksum for the playlist manifest
    playlist_length = os.path.getsize(temp_file)  # Get the file size in bytes

    # Create the playlist asset
    playlist_asset = VcatTestVectorPlaylistAsset(
        name=playlist_name,
        url=f"https://{bucket_name}.s3.amazonaws.com/{file_path}",  # URL for the playlist file
        checksum=playlist_checksum,  # Checksum for the playlist manifest
        uuid=playlist_uuid,
        length_bytes=playlist_length,

        description=playlist_description
    )

    return playlist_asset


def generate_playlist_catalog(bucket_url: str):
    """
    Generate a playlist catalog for the VCAT Demo Test Assets.
    """
    # Get all manifests from the S3 bucket
    all_manifests = get_all_manifests_from_s3(bucket_url)

    # Create the list of VcatTestVectorPlaylistAsset (only for playlist manifests)
    playlist_assets = []
    for manifest_file in all_manifests:
        if manifest_file.endswith('_playlist.json'):
            # Process the playlist manifest and add to the list
            playlist_asset = process_playlist_manifest(manifest_file, bucket_url)
            if playlist_asset:
                playlist_assets.append(playlist_asset)

    # Create the playlist catalog header
    header = VcatTestVectorHeader(
        name="VCAT Demo Test Assets",
        description="A collection of VCAT playlists",
        created_by="RoncaTech, LLC",
    )

    # Create the playlist catalog
    playlist_catalog = VcatTestVectorPlaylistCatalog(
        vcat_testvector_header=header,
        playlists=playlist_assets
    )

    # Ensure the './manifests' directory exists, create it if necessary
    output_dir = './manifests'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save the final playlist catalog to a JSON file
    output_file = os.path.join(output_dir, 'vcat_testvector_playlist_catalog.json')

    # Write the JSON to the file
    with open(output_file, 'w') as json_file:
        json.dump(playlist_catalog.to_dict(), json_file, indent=4)

    print(f"Playlist catalog saved as {output_file}")


def main():
    # Generate the playlist catalog
    generate_playlist_catalog(bucket_url)


if __name__ == "__main__":
    main()
