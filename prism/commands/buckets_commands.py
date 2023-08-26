import click
import uuid
import logging
import gzip
import shutil
import json
import os

logger = logging.getLogger(__name__)


def buckets_generate_impl():
    return "cli_" + uuid.uuid4().hex


@click.command("generate", help="Generate a unique bucket name.")
def buckets_generate():
    print(buckets_generate_impl())


@click.command("list", help="View the buckets permitted by the security profile of the current user.")
@click.option("-w", "--wid",
              help="The Workday ID of the bucket.")
@click.option("-n", "--table_name",
              help="The API name of the table to retrieve (see search option).")
@click.option("-l", "--limit", default=None,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-t", "--type", "type_", default="summary", show_default=True,
              help="How much information to be returned in response JSON.")
@click.option("-s", "--search", is_flag=True, show_default=True, default=False,
              help="Use contains search substring for --table_name or --id.")
@click.argument("bucket_name", required=False)
@click.pass_context
def buckets_list(ctx, wid, table_name, limit, offset, type_, search, bucket_name):
    p = ctx.obj["p"]

    buckets = p.buckets_list(wid, bucket_name, limit, offset, type_, table_name, search)

    print(json.dumps(buckets, indent=2))


@click.command("create", help="Create a new bucket with the specified name.")
@click.option("-n", "--table_name", default=None,
              help="Name of the table to associate with the bucket.")
@click.option("-w", "--table_wid", default=None,
              help="Table ID to associate with the table.")
@click.option("-f", "--file", "file_", required=False, default=None, type=click.Path(exists=True),
              help="Schema JSON file for the target table.")
@click.option("-o", "--operation", default="TruncateandInsert", show_default=True,
              help="Operation to perform on the table.")
@click.argument("bucket_name")
@click.pass_context
def buckets_create(ctx, table_name, table_wid, file_, operation, bucket_name):
    p = ctx.obj["p"]

    bucket = buckets_create_impl(p, bucket_name, table_wid, file_, operation)

    print(bucket)


@click.command("upload", help="Upload a CSV or gzip file to the specified bucket.")
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
@click.option("-b", "--bucket", help="Bucket name to create.")
@click.option("-c", "--complete", is_flag=True, default=False,
              help="Automatically complete bucket and load the data into the table.")
@click.argument("file", nargs=-1, required=True, type=click.Path(exists=True))
@click.pass_context
def buckets_upload(ctx, table_name, table_wid, schema_file, operation, generate, bucket, complete, file):
    p = ctx.obj["p"]

    # We know we have valid file name.  Check to see if we have a gzip file or a CSV
    # by checking the extension.

    if file is None:
        logger.error("An existing file name is required to upload to a bucket.")
        return

    source_file = file[0]
    target_file = source_file

    if source_file.lower().endswith(".csv"):
        # GZIP the file into the same directory with the appropriate extension.
        target_file += ".gz"

        with open(source_file, 'rb') as f_in:
            with gzip.open(target_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    elif not source_file.lower().endswith(".gz"):
        logger.error(f"File {target_file} is not a .gz or .csv file.")
        return

    # We think we have a file - we don't test the contents.
    # Go ahead and create a new bucket or use an existing.
    bucket = buckets_create_impl(p, bucket, table_name, table_wid, schema_file, operation)

    if bucket is None:
        logger.error("Invalid bucket for upload operation.")
        return

    upload = p.buckets_upload(bucket["id"], target_file)

    if upload is not None and complete:
        complete = p.buckets_complete(bucket["id"])


@click.command("complete", help="Complete the specified bucket and load any files in the bucket.")
@click.option("-n", "--bucket_name",
              help="Bucket to complete.")
@click.argument("bucket_wid", required=False)
@click.pass_context
def buckets_complete(ctx, bucket_name, bucket_wid):
    p = ctx.obj["p"]

    if bucket_wid is None and bucket_name is None:
        print("Either a bucket wid or a bucket name must be specified.")
        return

    if bucket_wid is not None:
        # If the caller passed both a name and WID, then
        # use the WID first.
        buckets = p.buckets_list(bucket_id=bucket_wid)
    else:
        # Lookup the bucket by name.
        buckets = p.buckets_list(bucket=bucket_name, verbosity="full")

    if buckets["total"] == 0:
        logger.error('Bucket not found.')
        return

    bucket = buckets["data"][0]

    bucket_state = bucket["state"]["descriptor"]

    if bucket_state != "New":
        print(f"Bucket state is \"{bucket_state}\" - only valid state is \"New.\"")
        return

    bucket_wid = bucket["id"]

    return p.buckets_complete(bucket_wid)


def buckets_create_impl(prism, bucket_name, table_name, table_wid, schema_file, operation):
    if bucket_name is not None:
        # Let's see if this bucket already exists
        buckets = prism.buckets_list(bucket=bucket_name)

        if buckets is not None and buckets["total"] != 0:
            logger.warning(f"Bucket {bucket_name} already exists - status: .")
            return buckets["data"][0]
    else:
        # Generate a unique bucket name for this operation.
        bucket = buckets_generate_impl()
        logger.debug(f"New bucket name: {bucket}")

    # A target table must be named and must exist.

    if table_name is None and table_wid is None:
        print("A table name or wid must be specified to create a bucket.")
        return None

    if table_name is not None:
        tables = prism.tables_list(api_name=table_name, type_="full")
    else:
        tables = prism.tables_list(wid=table_wid, type_="full")

    if tables["total"] != 1:
        print("Table for create bucket not found.")
        return

    table = tables["data"][0]

    if schema_file is not None:
        schema = prism.table_to_bucket_schema(load_schema(schema_file))
    else:
        schema = prism.table_to_bucket_schema(table)

    bucket = prism.buckets_create(bucket, table["id"], schema, operation=operation)

    return bucket


def load_schema(filename):
    """Load a table schema from a JSON file.

    :param filename:
    :return:
    """
    if not os.path.isfile(filename):
        logger.critical("Schema file not found: {filename}")
        return None

    try:
        with open(filename) as file:
            schema = json.load(file)

        # Check to see if this is a full table definition
        # or just a list of fields.

        if type(schema) is list:
            schema = {"fields": schema}
    except Exception as e:
        logger.critical("Invalid schema: %s".format(str(e)))
        pass

    return None
