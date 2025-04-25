import typer
from cli import (
    init_client,
    list_buckets,
    create_bucket,
    delete_bucket,
    bucket_exists,
    download_file_and_upload_to_s3,
    set_object_access_policy,
    create_bucket_policy,
    read_bucket_policy,
)

app = typer.Typer()

@app.command()
def buckets():
    """List all S3 buckets"""
    client = init_client()
    result = list_buckets(client)
    typer.echo(result)

@app.command()
def create(bucket_name: str, region: str = "us-west-2"):
    """Create a new S3 bucket"""
    client = init_client()
    success = create_bucket(client, bucket_name, region)
    typer.echo("Bucket created" if success else "Failed to create bucket")

@app.command()
def delete(bucket_name: str):
    """Delete an S3 bucket"""
    client = init_client()
    success = delete_bucket(client, bucket_name)
    typer.echo("Bucket deleted" if success else "Failed to delete bucket")

@app.command()
def exists(bucket_name: str):
    """Check if a bucket exists"""
    client = init_client()
    typer.echo("Exists" if bucket_exists(client, bucket_name) else "Does not exist")

@app.command()
def upload(bucket_name: str, url: str, file_name: str, keep_local: bool = False):
    """Download file from URL and upload to S3"""
    client = init_client()
    link = download_file_and_upload_to_s3(client, bucket_name, url, file_name, keep_local)
    typer.echo(f"Uploaded: {link}")

@app.command()
def make_public(bucket_name: str, file_name: str):
    """Make an uploaded object publicly readable"""
    client = init_client()
    success = set_object_access_policy(client, bucket_name, file_name)
    typer.echo("Object is public" if success else "Failed to update ACL")

@app.command()
def public_policy(bucket_name: str):
    """Apply a public read bucket policy"""
    client = init_client()
    create_bucket_policy(client, bucket_name)

@app.command()
def read_policy(bucket_name: str):
    """Read the current bucket policy"""
    client = init_client()
    read_bucket_policy(client, bucket_name)

if __name__ == "__main__":
    app()
