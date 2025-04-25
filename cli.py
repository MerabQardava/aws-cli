
import json
import os
import math
import mimetypes
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
import boto3
from dotenv import load_dotenv
from os import getenv

load_dotenv()




def init_client():
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=getenv("aws_access_key_id"),
            aws_secret_access_key=getenv("aws_secret_access_key"),
            aws_session_token=getenv("aws_session_token"),
            region_name=getenv("aws_region_name"))
        # Check if credentials are correct
        client.list_buckets()

        return client
    except ClientError as e:
        print(e)
        raise e


def list_buckets(aws_s3_client):
    try:
        return aws_s3_client.list_buckets()
    except ClientError as e:
        print(e)
        return False


def create_bucket(aws_s3_client, bucket_name,
                  region="us-west-2"):
    try:
        location = {'LocationConstraint': region}
        response = aws_s3_client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration=location)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def delete_bucket(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def bucket_exists(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def download_file_and_upload_to_s3(aws_s3_client,
                                   bucket_name,
                                   url,
                                   file_name,
                                   keep_local=False):
    from urllib.request import urlopen
    import io
    with urlopen(url) as response:
        content = response.read()
        try:
            aws_s3_client.upload_fileobj(
                Fileobj=io.BytesIO(content),
                Bucket=bucket_name,
                ExtraArgs={'ContentType': 'image/jpg'},
                Key=file_name)
        except Exception as e:
            print(e)

    if keep_local:
        with open(file_name, mode='wb') as file:
            file.write(content)

    return f"https://s3-us-west-2.amazonaws.com/{bucket_name}/{file_name}"


def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    try:
        response = aws_s3_client.put_object_acl(ACL="public-read",
                                                Bucket=bucket_name,
                                                Key=file_name)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def generate_public_read_policy(bucket_name):
    policy = {
        "Version":
        "2012-10-17",
        "Statement": [{
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket_name}/*",
        }],
    }

    return json.dumps(policy)


def create_bucket_policy(aws_s3_client, bucket_name):
    aws_s3_client.delete_public_access_block(Bucket=bucket_name)
    aws_s3_client.put_bucket_policy(
        Bucket=bucket_name, Policy=generate_public_read_policy(bucket_name))
    print("Bucket policy created successfully")


def read_bucket_policy(aws_s3_client, bucket_name):
    try:
        policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = policy["Policy"]
        print(policy_str)
    except ClientError as e:
        print(e)
        return False

def _get_content_type(path):
    return mimetypes.guess_type(path)[0]


def upload_small_file(s3_client, bucket, path, object_name=None):
    object_name = object_name or os.path.basename(path)
    extra_args = {}
    if (ctype := _get_content_type(path)):
        extra_args['ContentType'] = ctype


    try:
        s3_client.upload_file(
            Filename=path,
            Bucket=bucket,
            Key=object_name,
            ExtraArgs=extra_args
        )
        return True
    except ClientError as err:
        print(f"upload_small_file failed: {err}")
        return False


def upload_large_file(s3_client, bucket, path, object_name=None, chunk_size=10 * 1024 * 1024):
    object_name = object_name or os.path.basename(path)
    size = os.path.getsize(path)


    if size < 100 * 1024 * 1024:
        return upload_small_file(s3_client, bucket, path, object_name)


    content_type = _get_content_type(path) or 'application/octet-stream'


    try:
        multipart = s3_client.create_multipart_upload(
            Bucket=bucket,
            Key=object_name,
            ContentType=content_type
        )
        upload_id = multipart['UploadId']
        total_parts = math.ceil(size / chunk_size)
        uploaded_parts = []
        lock = Lock()


        def _upload_part(part_number):
            offset = (part_number - 1) * chunk_size
            length = min(chunk_size, size - offset)
            with open(path, 'rb') as f:
                f.seek(offset)
                data = f.read(length)
            resp = s3_client.upload_part(
                Bucket=bucket,
                Key=object_name,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=data
            )
            with lock:
                uploaded_parts.append({'PartNumber': part_number, 'ETag': resp['ETag']})


        with ThreadPoolExecutor(max_workers=min(4, total_parts)) as executor:
            executor.map(_upload_part, range(1, total_parts + 1))


        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=object_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': sorted(uploaded_parts, key=lambda p: p['PartNumber'])}
        )
        return True


    except Exception as err:
        print(f"upload_large_file failed: {err}")
        if 'upload_id' in locals():
            s3_client.abort_multipart_upload(
                Bucket=bucket,
                Key=object_name,
                UploadId=upload_id
            )
        return False


def set_lifecycle_policy(s3_client, bucket, prefix='', days=120):
    rule = {
        'ID': f'Delete after {days} days',
        'Status': 'Enabled',
        'Prefix': prefix,
        'Expiration': {'Days': days}
    }
    config = {'Rules': [rule]}


    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=config
        )
        return True
    except ClientError as err:
        print(f"set_lifecycle_policy failed: {err}")
        return False


def delete_file(client, bucket_name, file_name):
    """Delete a file from the S3 bucket."""
    try:
        client.delete_object(Bucket=bucket_name, Key=file_name)
        return True
    except Exception as e:
        print(f"Error deleting file: {e}")
        return False

def is_versioning_enabled(client, bucket_name):
    """Check if versioning is enabled on the given bucket."""
    try:
        response = client.get_bucket_versioning(Bucket=bucket_name)
        return response.get("Status", "Not enabled")
    except Exception as e:
        return f"Error: {e}"


def get_file_versions(client, bucket_name, file_name):
    """Return a list of versions with timestamps for a given file."""
    try:
        response = client.list_object_versions(Bucket=bucket_name, Prefix=file_name)
        return response.get("Versions", [])
    except Exception as e:
        print("Error:", e)
        return []


def reupload_previous_version(client, bucket_name, file_name):
    """Re-upload the previous version of a file as a new version."""
    try:
        versions = get_file_versions(client, bucket_name, file_name)
        if len(versions) < 2:
            return False

        previous_version = versions[1]
        previous_version_id = previous_version["VersionId"]

        obj = client.get_object(Bucket=bucket_name, Key=file_name, VersionId=previous_version_id)
        content = obj["Body"].read()

        client.put_object(Bucket=bucket_name, Key=file_name, Body=content)
        return True
    except Exception as e:
        print("Error during reupload:", e)
        return False

def organize_bucket_by_extension(client, bucket_name):
    from collections import defaultdict

    try:
        response = client.list_objects_v2(Bucket=bucket_name)
        contents = response.get("Contents", [])
        moved_count = defaultdict(int)

        for obj in contents:
            key = obj["Key"]
            if "/" in key:
                continue

            if "." not in key:
                continue

            extension = key.split(".")[-1]
            new_key = f"{extension}/{key}"

            client.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": key},
                Key=new_key
            )

            client.delete_object(Bucket=bucket_name, Key=key)

            moved_count[extension] += 1

        return dict(moved_count)

    except Exception as e:
        return {"error": str(e)}
