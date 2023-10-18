import json
import logging
import sys
import click
import pandas as pd

from . import util as u

logger = logging.getLogger(__name__)


@click.command("list")
@click.option("-w", "--wid",
              help="The Workday ID of the bucket.")
@click.option("-n", "--table_name",
              help="The API name of the table to retrieve (see search option).")
@click.option("-l", "--limit", default=None, type=int,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None, type=int,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-t", "--type", "type_", default="summary", show_default=True,
              help="How much information to be returned in response JSON.")
@click.option("-s", "--search", is_flag=True, show_default=True, default=False,
              help="Use contains search substring for --table_name or --wid.")
@click.option("-f", "--format", "format_",
              type=click.Choice(['json', 'summary', 'schema', 'csv'], case_sensitive=False),
              default="json",
              help="Format output as JSON, summary, schema, or CSV.",
              )
@click.argument("bucket_name", required=False)
@click.pass_context
def buckets_list(ctx, wid, table_name, limit, offset, type_, search, format_, bucket_name):
    """
    View the buckets permitted by the security profile of the current user.

    [BUCKET_NAME] explicit name of bucket to list.
    """

    p = ctx.obj["p"]

    buckets = p.buckets_list(wid, bucket_name, limit, offset, type_, table_name, search)

    if buckets["total"] == 0:
        return

    if format_ == "summary":
        for bucket in buckets["data"]:
            display_name = bucket["displayName"]
            operation = bucket["operation"]["descriptor"]
            target = bucket["targetDataset"]["descriptor"]
            state = bucket["state"]["descriptor"]

            click.echo(f"{display_name}, operation: {operation}, target: {target}, state: {state}")
    elif format_ == "csv":
        df = pd.json_normalize(buckets["data"])
        click.echo(df.to_csv(index=False))
    else:
        click.echo(json.dumps(buckets, indent=2))


@click.command("create")
@click.option("-n", "--table_name", default=None,
              help="Table name to associate with the bucket.")
@click.option("-w", "--table_wid", default=None,
              help="Table ID to associate with the table.")
@click.option("-f", "--file", "file_", required=False, default=None, type=click.Path(exists=True),
              help="Schema JSON file for the target table.")
@click.option("-o", "--operation", default="TruncateAndInsert", show_default=True,
              help="Operation to perform on the table.")
@click.argument("bucket_name", required=False)
@click.pass_context
def buckets_create(ctx, table_name, table_wid, file_, operation, bucket_name):
    """
    Create a new bucket with the specified name.

    [BUCKET_NAME] explicit bucket name to create otherwise default.
    """
    p = ctx.obj["p"]

    if table_name is None and table_wid is None and file_ is None:
        click.echo("A table must be associated with this bucket (-n, -w, or -f must be specified).")
        sys.exit(1)

    bucket = p.buckets_create(name=bucket_name, target_wid=table_wid, target_name=table_name, schema=file_, operation=operation)

    if bucket is not None:
        click.echo(json.dumps(bucket,indent=2))


@click.command("upload")
@click.option("-n", "--table_name", default=None,
              help="Name of the table to associate with the bucket.")
@click.option("-w", "--table_wid", default=None,
              help="Table ID to associate with the table.")
@click.option("-s", "--schema_file", default=None,
              help="Schema JSON file for the target table.")
@click.option("-o", "--operation", default="TruncateandInsert", show_default=True,
              help="Operation to perform on the table.")
@click.option("-g", "--generate", is_flag=True, default=True,
              help="Generate a unique bucket name.")
@click.option("-b", "--bucket", help="Bucket name to load files.")
@click.option("-c", "--complete", is_flag=True, default=False,
              help="Automatically complete bucket and load the data into the table.")
@click.argument("file", nargs=-1, required=True, type=click.Path(exists=True))
@click.pass_context
def buckets_upload(ctx, table_name, table_wid, schema_file, operation, generate, bucket, complete, file):
    """
    Upload a CSV or gzip file to the specified bucket

    [FILE] one or more gzip (.gz) or CSV (.csv) files.

    NOTE: This operation will create ".csv.gz" files for each .csv file.
    """
    p = ctx.obj["p"]

    # Convert the file(s) provided to a list of compressed files.
    target_files = u.compress_files(file)

    if len(target_files) == 0:
        click.echo("No files to upload.")
        sys.exit(1)

    # We think we have a file(s) - we don't test the contents.
    # Go ahead and create a new bucket or use an existing.
    bucket = p.buckets_create(bucket, table_name, table_wid, schema_file, operation)

    if bucket is None:
        logger.error("Invalid bucket for upload operation.")
        sys.exit(1)

    upload = p.buckets_upload(bucket["id"], target_files)

    if upload is None:
        logger.error("Upload failed.")
        sys.exit(1)

    if complete:
        complete = p.buckets_complete(bucket["id"])
        click.echo(complete)
    else:
        click.echo(upload)

@click.command("complete")
@click.option("-n", "--bucket_name",
              help="Bucket to complete.")
@click.argument("bucket_wid", required=False)
@click.pass_context
def buckets_complete(ctx, bucket_name, bucket_wid):
    """
    Complete the specified bucket and perform the specified operation.

    [BUCKET_WID] the Workday ID of the bucket to complete.
    """
    p = ctx.obj["p"]

    if bucket_wid is None and bucket_name is None:
        click.echo("A bucket wid or a bucket name must be specified.")
        sys.exit(1)

    if bucket_wid is not None:
        # If the caller passed both a name and WID, then use the WID first.
        buckets = p.buckets_list(bucket_id=bucket_wid)
    else:
        # Lookup the bucket by name.
        buckets = p.buckets_list(bucket=bucket_name, verbosity="full")

    if buckets["total"] == 0:
        logger.error('Bucket not found.')
        sys.exit(1)

    bucket = buckets["data"][0]

    bucket_state = bucket["state"]["descriptor"]

    if bucket_state != "New":
        click.echo(f"Bucket state is \"{bucket_state}\" - only \"New.\" buckets can be completed.")
        sys.exit(1)

    click.echo(p.buckets_complete(bucket["id"]))


@click.command("status")
@click.option("-w", "--wid", required=False, help="Bucket wid to status")
@click.argument("name", required=False)
@click.pass_context
def buckets_status(ctx, name, wid):
    """
    Get the status of a bucket by name or workday ID.

    [NAME] name of bucket.
    """
    p=ctx.obj["p"]

    buckets=p.buckets_list(wid, bucket_name=name)

    if buckets["total"] == 1:
        click.echo(buckets["data"][0]["state"]["descriptor"])


@click.command("name")
@click.pass_context
def buckets_name(ctx):
    """
    Generate a bucket name to use for other bucket operations.
    """
    click.echo(ctx.obj["p"].buckets_gen_name())