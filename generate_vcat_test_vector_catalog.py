import boto3
import json
import hashlib
import tempfile
import os

S3_BUCKET = "roncatech-vcat-test-vectors"
MANIFEST_PREFIX = "manifests/"
CATALOG_KEY = "manifest_catalog.json"

s3 = boto3.client("s3")

def download_and_hash_manifest(s3_key):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        s3.download_file(S3_BUCKET, s3_key, tmp.name)
        with open(tmp.name, "rb") as f:
            data = f.read()
            sha256 = hashlib.sha256(data).hexdigest()
            try:
                parsed = json.loads(data)
            except Exception as e:
                print(f"❌ Error parsing {s3_key}: {e}")
                return None, None
            return parsed, sha256
    finally:
        tmp.close()
        os.remove(tmp.name)

def build_catalog():
    print(f"📦 Scanning s3://{S3_BUCKET}/{MANIFEST_PREFIX}")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=MANIFEST_PREFIX)

    catalog = {
        "catalog_version": 1,
        "manifests": []
    }

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            manifest_json, sha = download_and_hash_manifest(key)
            if not manifest_json or "vcat-test-vector" not in manifest_json:
                print(f"⚠️ Skipping invalid or missing manifest: {key}")
                continue

            meta = manifest_json["vcat-test-vector"]
            entry = {
                "uuid": meta.get("uuid"),
                "description": meta.get("description"),
                "url": f"https://{S3_BUCKET}.s3.amazonaws.com/{key}",
                "sha256": sha
            }

            catalog["manifests"].append(entry)
            print(f"✅ Added: {entry['description']}")

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp:
        json.dump(catalog, tmp, indent=2)
        tmp_path = tmp.name

    s3.upload_file(tmp_path, S3_BUCKET, CATALOG_KEY, ExtraArgs={"ContentType": "application/json"})
    os.remove(tmp_path)
    print(f"\n📝 Uploaded catalog to s3://{S3_BUCKET}/{CATALOG_KEY}")

if __name__ == "__main__":
    build_catalog()
