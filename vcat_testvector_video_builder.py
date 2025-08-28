import os
import json
import uuid
from datetime import datetime
import subprocess  # For running ffmpeg to check the video codec
import boto3  # AWS SDK to interact with S3
import re  # Import re for regular expressions

import vcat_testvector_datamodels
from utils import getTempCopyFromS3, getChecksum, getFileLength
import settings as cfg

from urllib.parse import urlparse

def get_video_files(path):
    """
    Dispatch to get_video_files_from_s3 or get_video_files_from_folder based on `path`.

    Accepted inputs:
      - S3 URL starting with cfg.S3_URL (e.g., 's3://my-bucket/...').
      - Local folder path (absolute or relative).
      - file:// URL pointing to a local folder.

    Returns:
      A list of file keys/paths as returned by the underlying function.
    """
    if path is None:
        raise ValueError("path must be a non-empty string or PathLike")

    # Normalize path to string
    if isinstance(path, os.PathLike):
        path = os.fspath(path)

    # Expand ~ and environment variables for local paths
    path = os.path.expanduser(os.path.expandvars(path))

    parsed = urlparse(path)

    # S3: either explicit cfg.S3_URL prefix or URL scheme 's3'
    if path.startswith(getattr(cfg, "S3_URL", "s3://")) or parsed.scheme.lower() == "s3":
        return get_video_files_from_s3(path)

    # Local via file:// URL
    if parsed.scheme.lower() == "file":
        local_path = parsed.path
        return get_video_files_from_folder(local_path)

    # If some other URL scheme is present, it's unsupported here
    if "://" in path and parsed.scheme:
        raise ValueError(f"Unsupported URL scheme '{parsed.scheme}'. Only 's3://' or local paths are supported.")

    # Default: treat as local folder path
    return get_video_files_from_folder(path)


def get_video_files_from_s3(bucket_url):
    """
    Returns a list of video files in the 'media' directory of the provided bucket URL.
    Assumes that all files in the 'media' folder are video files.
    """
    # Parse the bucket URL to extract the bucket name and prefix
    if not bucket_url.startswith(cfg.S3_URL):
        raise ValueError(f"The URL must be an S3 URL starting with '{cfg.S3_URL}'.")

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

def get_video_files_from_folder(folderpath, *, exit_on_error=True, logger=None):
    """
    Return a list of files under '<folderpath>/media' (or 'folderpath' if it *is* the media folder).
    If the folder is not readable/traversable (macOS TCC, perms, etc.), log an error and exit.

    Args:
        folderpath (str | PathLike): Either the parent containing 'media/', or the 'media/' folder itself.
        exit_on_error (bool): If True, sys.exit on error; if False, raise exceptions.
        logger (logging.Logger | None): Logger to use; default is 'vcat.media'.
    """
    media_base = "media"

    # Normalize
    folderpath = os.path.expanduser(os.path.expandvars(os.fspath(folderpath)))
    base = os.path.normpath(folderpath)

    # Resolve media folder, supporting either parent or media itself
    as_media_child = os.path.join(base, media_base)
    if os.path.isdir(as_media_child):
        media_folder = as_media_child
    elif os.path.isdir(base) and os.path.basename(base) == media_base:
        media_folder = base
    else:
        msg = (f"The 'media' directory does not exist. Looked for:\n"
               f"  {as_media_child}\n"
               f"  {base} (as media folder)")
        print(logger, msg, FileNotFoundError(msg), exit_on_error, exit_code=2)
        return []

    # Permission preflight: need R (read) + X (traverse) on the directory
    r_ok = os.access(media_folder, os.R_OK)
    x_ok = os.access(media_folder, os.X_OK)
    if not (r_ok and x_ok):
        msg = (f"Cannot read/traverse media folder: {media_folder} "
               f"(R_OK={r_ok}, X_OK={x_ok}). "
               f"On macOS, grant Full Disk Access / Files & Folders to your Python/IDE "
               f"or move the folder out of Desktop/Documents/Downloads.")
        print(logger, msg, PermissionError(msg), exit_on_error, exit_code=13)
        return []

    # Force a scandir so PermissionError surfaces now (macOS TCC, etc.)
    try:
        top_entries = [e.name + ("/" if e.is_dir() else "") for e in os.scandir(media_folder)]
    except PermissionError as e:
        msg = (f"Permission denied reading '{media_folder}': {e}. "
               f"Hint: macOS privacy (TCC) often blocks Downloads/Desktop/Documents for apps "
               f"without permission. Grant access or move the folder. cwd={os.getcwd()}")
        print(logger, msg, e, exit_on_error, exit_code=13)
        return []

    video_files = []

    def _onerror(err):
        # Bubble up errors instead of silently skipping
        print(logger, f"Error walking '{media_folder}': {err}", err, exit_on_error, exit_code=14)
        return []

    for root, dirs, files in os.walk(media_folder, topdown=True, followlinks=True, onerror=_onerror):
        # keep traversal tidy on macOS
        dirs[:] = [d for d in dirs if d not in ('.DS_Store', '__MACOSX')]
        for name in files:
            if name == '.DS_Store':
                continue

            relative = os.path.join(media_base, os.path.relpath(root, media_folder))

            video_files.append(os.path.join(relative, name))

    if not video_files:
        msg = (f"No files found under {media_folder}. "
               f"Top-level entries: {top_entries} | cwd={os.getcwd()}")
        print(logger, msg, RuntimeError(msg), exit_on_error, exit_code=15)

    return video_files


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

def generate_video_manifest(video_path, bucket_url, created_by):
    """
    Dispatch to S3 vs local based solely on the scheme of a fully-resolved path.
      - S3:  s3://<bucket>/<key>
      - Local: absolute/relative filesystem path

    Calls:
      generate_video_manifest_s3(key, bucket_url, bucket_url, created_by)
      generate_video_manifest_local(local_path, bucket_url, created_by)
    """
    path_str = os.fspath(video_path)
    parsed = urlparse(path_str)

    if parsed.scheme.lower() == "s3":
        key = parsed.path.lstrip("/")
        return generate_video_manifest_s3(key, bucket_url, created_by)

    # Everything else is treated as local
    return generate_video_manifest_local(path_str, bucket_url, created_by)

def generate_video_manifest_s3(video_file, bucket_url, created_by):
    # download URL
    s3_url = f"{bucket_url}/{video_file}"
    print(f"Generated S3 URL for download: {s3_url}")
    try:
        tmp = getTempCopyFromS3(s3_url)
        do_generate_video_manifest(video_file, tmp, created_by)

    except Exception as e:
        print(f"Error during manifest generation: {e}")

def generate_video_manifest_local(video_file, bucket_url, created_by):
    local_file = os.path.join(bucket_url, video_file)
    do_generate_video_manifest(video_file, local_file, created_by)


def do_generate_video_manifest(video_file, local_file, created_by):

    # manifest URL
    video_url = f"../{video_file}"

    try:

        checksum     = getChecksum(local_file)
        length_bytes = getFileLength(local_file)

        # probe details
        video_mime_type, duration_ms, resolution_x_y, frame_rate = get_video_details(local_file)

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

        outDir = cfg.MANIFEST_DIR

        clean_name = video_file.split("/")[-1]
        out = f"{outDir}/{clean_name}_video_manifest.json"
        with open(out, "w") as f:
            json.dump(test_vector, f, indent=4)
        print(f"✔ Wrote {out}")

        print(f"Generated video URL for manifest: {video_url} with uuid {header.uuid}")


    except Exception as e:
        print(f"Error during manifest generation: {e}")


# Example usage
vector_url = "https://roncatech-vcat-test-vectors.s3.us-west-2.amazonaws.com"
bucket_url = cfg.BASE_OUTPUT_DIR  #"s3://roncatech-vcat-test-vectors"  # Replace with your S3 bucket URL
created_by = "RoncaTech, LLC"
description = "VCAT sample test vectors"

# Get the list of video files in the S3 bucket
video_files = get_video_files(bucket_url)

# Iterate over the video files and generate a manifest for each
for video_file in video_files:
    generate_video_manifest(video_file, bucket_url, created_by)
