import boto3

# Create a session using the default AWS credentials (this will use your configured IAM user)
session = boto3.Session()

# Create an S3 client using the session
s3_client = session.client('s3')

# Specify your bucket name
bucket_name = 'roncatech-vcat-test-vectors'

# List objects (folders/prefixes) in the bucket
response = s3_client.list_objects_v2(Bucket=bucket_name, Delimiter='/')

# Check if there are common prefixes (folders)
if 'CommonPrefixes' in response:
    print(f"Folders in bucket '{bucket_name}':")
    for prefix in response['CommonPrefixes']:
        folder_name = prefix['Prefix']
        folder_url = f"s3://{bucket_name}/{folder_name}"
        print(f"  Folder URL: {folder_url}")
else:
    print(f"No folders found in the bucket '{bucket_name}'.")
