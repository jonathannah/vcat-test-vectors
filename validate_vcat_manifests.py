import json
import hashlib
import boto3
import tempfile
from io import BytesIO

# --- Configuration ---
S3_BUCKET = "roncatech-vcat-test-vectors"
S3_MANIFEST_PREFIX = "manifests/"

s3 = boto3.client("s3")

def compute_sha256(file_path):
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def validate_manifest_from_s3(key):
    print(f"\n📄 Validating: {key}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    manifest = json.load(obj["Body"])

    vector = manifest.get("vcat-test-vector")
    if not vector:
        print("❌ Invalid format: missing 'vcat-test-vector'")
        return

    for media in vector.get("media", []):
        url = media.get("url")
        expected_sha = media.get("sha256")
        filename = media.get("filename")

        if not url or not filename:
            print(f"❌ Invalid media entry: missing filename or url")
            continue

        print(f"⬇️  Downloading {filename} ...")
        tmp_path = tempfile.mktemp(suffix=filename)

        try:
            s3_key = url.split(f"https://{S3_BUCKET}.s3.amazonaws.com/")[-1]
            s3.download_file(S3_BUCKET, s3_key, tmp_path)

            actual_sha = compute_sha256(tmp_path)
            if actual_sha == expected_sha:
                print(f"✅ {filename} verified OK")
            else:
                print(f"❌ {filename} checksum mismatch")
        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")


def main():
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_MANIFEST_PREFIX)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                validate_manifest_from_s3(key)

if __name__ == "__main__":
    main()
