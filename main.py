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
    delete_file, get_file_versions, reupload_previous_version, organize_bucket_by_extension,
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

@app.command()
def delete_bucket_file(bucket_name: str, file_key: str,delete: bool = typer.Option(False, "--del", help="Flag to confirm deletion")):
    if not delete:
        typer.echo("Use --del flag to confirm deletion")
        raise typer.Exit(1)

    client = init_client()
    if delete_file(client, bucket_name, file_key):
        typer.echo(f"Successfully deleted {file_key} from {bucket_name}")
    else:
        typer.echo(f"Failed to delete {file_key}")
        raise typer.Exit(1)


@app.command()
def versioning_status(bucket_name: str, check: bool = False):
    """Check if versioning is enabled for a bucket"""
    if check:
        client = init_client()
        response = client.get_bucket_versioning(Bucket=bucket_name)
        status = response.get("Status", "Not enabled")
        typer.echo(f"Versioning status: {status}")
    else:
        typer.echo("Use --check to verify versioning status.")


@app.command()
def list_versions(bucket_name, file_name, show_versions=False):
    """List versions of a file in a bucket"""
    if show_versions:
        client = init_client()
        versions = get_file_versions(client, bucket_name, file_name)
        typer.echo(f"Found {len(versions)} versions for {file_name}:\n")
        for v in versions:
            typer.echo(f"- VersionId: {v['VersionId']}, LastModified: {v['LastModified']}")
    else:
        typer.echo("Use --show-versions to display file versions.")

@app.command()
def reupload_previous(bucket_name, file_name, reupload=False):
    """Re-upload the previous version of a file as the latest version"""
    if reupload:
        client = init_client()
        success = reupload_previous_version(client, bucket_name, file_name)
        typer.echo("Re-uploaded previous version." if success else "Failed to re-upload.")
    else:
        typer.echo("Use --reupload to re-upload the previous version.")


@app.command()
def organize(bucket_name, organize: bool = False):
    """Organize files by extension into folders and count movements"""
    if organize:
        client = init_client()
        result = organize_bucket_by_extension(client, bucket_name)

        if "error" in result:
            typer.echo(f"Error: {result['error']}")
        else:
            typer.echo("Moved files by extension:")
            for ext, count in result.items():
                typer.echo(f"{ext} - {count}")
    else:
        typer.echo("Use --organize to trigger organization")

@app.command()
def create_static_website_cmd(bucket_name: str, file_name: str):
    client = init_client()

    if not create_bucket(client, bucket_name):
        typer.echo("Failed to create bucket")
        raise typer.Exit(1)

    try:
        client.delete_public_access_block(
            Bucket=bucket_name
        )

        website_configuration = {
            'ErrorDocument': {'Key': 'error.html'},
            'IndexDocument': {'Suffix': 'index.html'},
        }
        client.put_bucket_website(Bucket=bucket_name, WebsiteConfiguration=website_configuration)

        client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=generate_public_read_policy(bucket_name)
        )

        if upload_small_file(client, bucket_name, file_name):
            typer.echo(f"Successfully configured static website hosting for {bucket_name}")
            typer.echo(f"Website URL: http://{bucket_name}.s3-website-{client.meta.region_name}.amazonaws.com")
        else:
            typer.echo("Failed to upload file")

    except ClientError as e:
        typer.echo(f"Error configuring website: {e}")
        raise typer.Exit(1)



if __name__ == "__main__":
    app()
