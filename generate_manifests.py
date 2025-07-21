import os
import json
import uuid
import hashlib
import boto3
from pathlib import Path
from datetime import datetime

# --- Configuration ---
S3_BUCKET = "roncatech-vcat-test-vectors"
S3_MEDIA_PREFIX = "media/"
S3_MANIFEST_PREFIX = "manifests/"

s3 = boto3.client("s3")

def generate_presigned_url(bucket, key):
    return f"https://{bucket}.s3.amazonaws.com/{key}"

def compute_s3_sha256(bucket, key):
    # You must download file to compute SHA256
    tmp_path = f"/tmp/{os.path.basename(key)}"
    s3.download_file(bucket, key, tmp_path)
    h = hashlib.sha256()
    with open(tmp_path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    os.remove(tmp_path)
    return h.hexdigest()

def upload_manifest_to_s3(content, manifest_name):
    s3_key = f"{S3_MANIFEST_PREFIX}{manifest_name}"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(content, indent=2),
        ContentType="application/json"
    )
    print(f"☁️  Uploaded: s3://{S3_BUCKET}/{s3_key}")

def build_manifests_from_s3():
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_MEDIA_PREFIX)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".mp4"):
                continue

            filename = os.path.basename(key)
            subfolder = os.path.dirname(key).split("/")[-1]
            url = generate_presigned_url(S3_BUCKET, key)
            sha256 = compute_s3_sha256(S3_BUCKET, key)

            manifest = {
                "vcat-test-vector": {
                    "uuid": str(uuid.uuid4()),
                    "description": filename,
                    "createdAt": datetime.utcnow().isoformat() + "Z",
                    "media": [
                        {
                            "filename": filename,
                            "url": url,
                            "sha256": sha256
                        }
                    ],
                    "playlists": []
                }
            }

            manifest_name = f"{filename.replace('.mp4', '')}.vcat-manifest.json"
            upload_manifest_to_s3(manifest, manifest_name)

def main():
    build_manifests_from_s3()

if __name__ == "__main__":
    main()
