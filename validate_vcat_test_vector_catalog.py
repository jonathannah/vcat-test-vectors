import boto3
import json
import hashlib
import requests
from pathlib import Path

S3_BUCKET = "roncatech-vcat-test-vectors"
CATALOG_KEY = "vcat_testvector_playlist_catalog.json"

s3 = boto3.client("s3")


def fetch_catalog():
    print(f"📥 Downloading catalog: s3://{S3_BUCKET}/{CATALOG_KEY}")
    local_path = Path("/tmp/manifest_catalog.json")
    s3.download_file(S3_BUCKET, CATALOG_KEY, str(local_path))
    with open(local_path, "r") as f:
        return json.load(f)


def validate_entry(entry):
    print(f"\n📄 Validating: {entry['description']}")
    print(f"⬇️  Downloading manifest: {entry['url']}")

    try:
        response = requests.get(entry["url"])
        response.raise_for_status()
        content = response.content
    except Exception as e:
        print(f"❌ Failed to download: {e}")
        return False

    hash_val = hashlib.sha256(content).hexdigest()
    if hash_val != entry["sha256"]:
        print(f"❌ Checksum mismatch for {entry['description']}")
        print(f"   Expected: {entry['sha256']}")
        print(f"   Found:    {hash_val}")
        return False

    try:
        parsed = json.loads(content)
        if "vcat-test-vector" not in parsed:
            print("❌ Missing 'vcat-test-vector' key")
            return False
    except Exception as e:
        print(f"❌ JSON parse error: {e}")
        return False

    print(f"✅ Manifest verified: {entry['description']}")
    return True


def main():
    catalog = fetch_catalog()
    manifests = catalog.get("manifests", [])
    print(f"🔍 Validating {len(manifests)} manifests...")

    passed = 0
    for entry in manifests:
        if validate_entry(entry):
            passed += 1

    print(f"\n✅ {passed}/{len(manifests)} manifests passed validation.")


if __name__ == "__main__":
    main()
