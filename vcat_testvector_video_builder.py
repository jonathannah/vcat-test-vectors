import os
import json
import uuid
from datetime import datetime
import subprocess  # For running ffmpeg to check the video codec
import boto3  # AWS SDK to interact with S3
import re  # Import re for regular expressions

import vcat_testvector_datamodels
from utils import getTempCopyFromS3, getChecksum, getFileLength


def get_video_files(bucket_url):
    """
    Returns a list of video files in the 'media' directory of the provided bucket URL.
    Assumes that all files in the 'media' folder are video files.
    """
    # Parse the bucket URL to extract the bucket name and prefix
    if not bucket_url.startswith("s3://"):
        raise ValueError("The URL must be an S3 URL starting with 's3://'.")

    parts = bucket_url[5:].split("/", 1)
    bucket_name = parts[0]
    prefix = "media/"  # Force it to only look in the 'media' directory

    # Use AWS SDK (boto3) to list the files in the S3 bucket
    s3_client = boto3.client('s3')
    files = []

    # Paginate through the results if there are many files
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                files.append(obj["Key"])  # Store file key (path within the bucket)

    return files


def get_video_details(file_path):
    """
    Get the video codec, duration, resolution, and frame rate from a video file using FFmpeg.
    """
    try:
        # Run ffmpeg to get codec, duration, resolution, and frame rate info
        command = ["ffmpeg", "-i", file_path]
        result = subprocess.run(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

        # Parse the stderr output for codec, duration, resolution, and frame rate information
        stderr_output = result.stderr

        # Get the codec information (look specifically for 'Video: av1' or 'Video: vp9')
        if "Video: av1" in stderr_output:  # Look for 'av1' codec specifically
            codec = "video/av1"
        elif "Video: vp9" in stderr_output:  # Look for 'vp9' codec specifically
            codec = 'video/mp4; codecs="vp09"'
        else:
            codec = "Unknown"

        # Extract duration (in seconds) and convert it to milliseconds
        duration_ms = None
        duration_match = re.search(r"Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2})", stderr_output)
        if duration_match:
            hours = int(duration_match.group(1))
            minutes = int(duration_match.group(2))
            seconds = int(duration_match.group(3))
            milliseconds = int(duration_match.group(4)) * 10  # FFmpeg provides 2-digit precision in ms
            duration_ms = (hours * 3600 + minutes * 60 + seconds) * 1000 + milliseconds

        # Extract resolution (width x height)
        resolution_x_y = None
        resolution_match = re.search(r", (\d+)x(\d+),", stderr_output)
        if resolution_match:
            resolution_x_y = f"{resolution_match.group(1)}X{resolution_match.group(2)}"

        # Extract frame rate (e.g., 29.97 fps)
        frame_rate = None
        frame_rate_match = re.search(r"(\d+(\.\d+)?) fps", stderr_output)
        if frame_rate_match:
            frame_rate = float(frame_rate_match.group(1))  # Capture the frame rate as a float
        else:
            frame_rate = "unknown"

        print(f"Resolution: {resolution_x_y}, Frame Rate: {frame_rate}")  # Debugging output for resolution and frame rate

        return codec, duration_ms, resolution_x_y, frame_rate

    except Exception as e:
        unknown = "unknown"
        print(f"Error getting video details: {e}")
        return unknown, unknown, unknown, unknown


def generate_header_title(video_file: str, video_mime_type: str, resolution_x_y: str, frame_rate: int) -> str:
    # Determine the base name for the header based on the video mime type
    base_name = ""

    # Handle AV1 mime type
    if 'av1' in video_mime_type.lower():
        base_name = f"av1-{resolution_x_y}p{frame_rate}"

    # Handle VP9 mime type
    elif 'vp09' in video_mime_type.lower():
        base_name = f"vp9-{resolution_x_y}p{frame_rate}"

    # Default behavior if mime type is neither av1 nor vp9
    if not base_name:
        base_name = video_file.split('/')[-1]  # Get the last part after the last '/'

    # Handle the '-fd0/1/2' suffix based on video file name
    if 'fd0' in video_file:
        base_name += '-fd0'
    elif 'fd1' in video_file:
        base_name += '-fd1'
    elif 'fd2' in video_file:
        base_name += '-fd2'

    return base_name


def generate_video_manifest(video_file, bucket_url, created_by):
    video_mime_type = "video/mp4"
    duration_ms = ""
    resolution_x_y = "1234X5678"

    # manifest URL
    video_url = f"{vector_url}/{video_file}"

    # download URL
    s3_url = f"{bucket_url}/{video_file}"
    print(f"Generated S3 URL for download: {s3_url}")

    try:
        tmp = getTempCopyFromS3(s3_url)
        checksum     = getChecksum(tmp)
        length_bytes = getFileLength(tmp)

        # probe details
        video_mime_type, duration_ms, resolution_x_y, frame_rate = get_video_details(tmp)

        header_title = generate_header_title(video_file, video_mime_type, resolution_x_y, frame_rate)
        header_description = (
            f"VCAT Test asset: {video_mime_type}, "
            f"{resolution_x_y}, {frame_rate}fps, {duration_ms}ms"
        )
        header = vcat_testvector_datamodels.VcatTestVectorHeader(
            header_title, header_description, created_by
        )

        media_asset = vcat_testvector_datamodels.VcatTestVectorVideoAsset(
            video_file, video_url,
            checksum, length_bytes,
            video_mime_type, duration_ms,
            resolution_x_y, frame_rate
        )

        test_vector = {
            "vcat_testvector_header": header.to_dict(),
            "media_asset": media_asset.to_dict()
        }

        os.makedirs("./manifests", exist_ok=True)
        clean_name = video_file.split("/")[-1]
        out = f"./manifests/{clean_name}_video_manifest.json"
        with open(out, "w") as f:
            json.dump(test_vector, f, indent=4)
        print(f"✔ Wrote {out}")

        print(f"Generated video URL for manifest: {video_url} with uuid {header.uuid}")


    except Exception as e:
        print(f"Error during manifest generation: {e}")


# Example usage
vector_url = "https://roncatech-vcat-test-vectors.s3.us-west-2.amazonaws.com"
bucket_url = "s3://roncatech-vcat-test-vectors"  # Replace with your S3 bucket URL
created_by = "RoncaTech, LLC"
description = "VCAT sample test vectors"

# Get the list of video files in the S3 bucket
video_files = get_video_files(bucket_url)

# Iterate over the video files and generate a manifest for each
for video_file in video_files:
    generate_video_manifest(video_file, bucket_url, created_by)
