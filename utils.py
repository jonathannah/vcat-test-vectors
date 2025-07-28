import hashlib
import os
import boto3  # AWS SDK to interact with S3
import tempfile  # For creating temporary files

def getTempCopyFromS3(s3_url):
    """
    Downloads a file from S3 to a temporary local file and returns the local file path.
    The S3 URL is expected in the format: s3://bucket-name/path/to/file
    """
    # Ensure the URL starts with 's3://'
    if not s3_url.startswith("s3://"):
        raise ValueError("The URL must start with 's3://'")

    # Strip the 's3://' prefix and split at the first '/'
    url_without_prefix = s3_url[5:]
    bucket_name, file_path = url_without_prefix.split("/", 1)

    # Create a temporary file to store the S3 file
    temp_file = tempfile.NamedTemporaryFile(delete=False)

    # Create an S3 client and download the file to the temporary file
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket_name, file_path, temp_file.name)

    # Return the path to the temporary file
    return temp_file.name


def getChecksum(local_file_path):
    """
    Calculate the checksum of a local file using SHA256.
    """
    hash_sha256 = hashlib.sha256()

    with open(local_file_path, "rb") as f:
        while chunk := f.read(4096):  # Read file in chunks to avoid memory overload
            hash_sha256.update(chunk)

    return hash_sha256.hexdigest()


def getFileLength(file_path, is_s3=False, bucket_name=None):
    """
    Get the length (size) of the file in bytes.
    If the file is on S3, it will fetch the file metadata instead.
    """
    if is_s3:
        s3_client = boto3.client('s3')
        response = s3_client.head_object(Bucket=bucket_name, Key=file_path)
        return response['ContentLength']  # Length in bytes
    else:
        return os.path.getsize(file_path)  # For local files, simply use os.path.getsize()
