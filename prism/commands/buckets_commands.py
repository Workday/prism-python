import json
import logging
import sys
import click

logger = logging.getLogger('prismCLI')


@click.command("get")
@click.option('-n', '--isName', is_flag=True, default=False,
              help='Flag to treat the bucket or table argument as a name.')
@click.option("-l", "--limit", default=None, type=int,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None, type=int,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-t", "--type", "type_", default="summary", show_default=True,
              help="How much information to be returned in response JSON.")
@click.option("-s", "--search", is_flag=True, show_default=True, default=False,
              help="Use substring search bucket or table.")
@click.option("--table",
              help="The id or name of a Prism table to list all buckets.")
@click.argument("bucket", required=False)
@click.pass_context
def buckets_get(ctx, bucket, table, isname,
                limit, offset, type_, search):
    """
    View the buckets permitted by the security profile of the current user.

    [BUCKET] ID or name of a Prism bucket.

    NOTE: For table name searching, this will be the Display Name not
    the API Name.
    """

    p = ctx.obj["p"]

    if isname and bucket is None and table is None:
        # It's invalid to add the --isName switch without providing a bucket name.
        logger.error('To get buckets by name, please provide a bucket name.')
        sys.exit(1)

    if not isname and bucket is not None:
        # This should be a bucket ID - ignore all other options.
        bucket = p.buckets_get(id=bucket, type_=type_)
        logger.info(json.dumps(bucket, indent=2))

        return

    # We are doing some form of search.

    if isname and bucket is not None:
        # This should be a search by bucket name.
        buckets = p.buckets_get(name=bucket, type_=type_, search=search)
    else:
        # Search by table ID or name.
        if isname:
            buckets = p.buckets_get(table_name=table, search=search,
                                    limit=limit, offset=offset, type_=type_)
        else:
            buckets = p.buckets_get(table_id=table,
                                    limit=limit, offset=offset, type_=type_)

    if buckets['total'] == 0:
        logger.info('No buckets found.')
    else:
        logger.info(json.dumps(buckets, indent=2))


@click.command("create")
@click.option("-n", "--target_name", default=None,
              help="Table name to associate with the bucket.")
@click.option("-i", "--target_id", default=None,
              help="Table ID to associate with the table.")
@click.option("-f", "--file", "file_", required=False, default=None, type=click.Path(exists=True),
              help="Schema JSON file for the target table.")
@click.option("-o", "--operation", default="TruncateAndInsert", show_default=True,
              help="Operation to perform on the table.")
@click.argument("name", required=False)
@click.pass_context
def buckets_create(ctx, target_name, target_id, file_, operation, name):
    """
    Create a new bucket with the specified name.

    [NAME] explicit bucket name to create otherwise default.
    """
    p = ctx.obj["p"]

    if target_name is None and target_id is None and file_ is None:
        logger.error("A table must be associated with this bucket (-n, -i, or -f must be specified).")
        sys.exit(1)

    bucket = p.buckets_create(name=name, target_id=target_id, target_name=target_name,
                              schema=file_, operation=operation)

    if bucket is not None:
        logger.info(json.dumps(bucket, indent=2))
    else:
        sys.exit(1)


@click.command("files")
@click.option("-n", "--target_name", default=None,
              help="Name of the table to associate with the bucket.")
@click.option("-i", "--target_id", default=None,
              help="Table ID to associate with the table.")
@click.option("-f", "--file", default=None,
              help="Schema JSON file for the target table.")
@click.option("-o", "--operation", default="TruncateAndInsert", show_default=True,
              help="Operation to perform on the table.")
@click.option("-b", "--bucket", help="Bucket name to load files.", default=None)
@click.option("-c", "--complete", is_flag=True, default=False,
              help="Automatically complete bucket and load the data into the table.")
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True))
@click.pass_context
def buckets_files(ctx, target_name, target_id, file, operation, bucket, complete, files):
    """
    Upload one or more CSV or gzip files to the specified bucket

    [FILES] one or more gzip (.gz) or CSV (.csv) files.

    NOTE: This operation will create ".csv.gz" files for each .csv file.
    """
    p = ctx.obj["p"]

    # We think we have a file(s) - we don't test the contents.
    # Go ahead and create a new bucket or use an existing.
    bucket = p.buckets_create(bucket, target_name, target_id, file, operation)

    if bucket is None:
        logger.error("Invalid bucket for upload operation.")
        sys.exit(1)

    results = p.buckets_files(bucket["id"], files)

    if results['total'] > 0 and complete:
        complete = p.buckets_complete(bucket["id"])
        logger.info(complete)
    else:
        logger.info(json.dumps(results, indent=2))


@click.command("complete")
@click.option("-n", "name",
              help="Bucket name to complete.")
@click.argument("id", required=False)
@click.pass_context
def buckets_complete(ctx, name, id):
    """
    Complete the specified bucket and perform the specified operation.

    [ID] A reference to a Prism Analytics bucket.
    """
    p = ctx.obj["p"]

    if id is None and name is None:
        click.echo("A bucket wid or a bucket name must be specified.")
        sys.exit(1)

    if id is not None:
        # If the caller passed both a name and WID, then use the WID first.
        bucket = p.buckets_list(bucket_id=id)
    else:
        # Lookup the bucket by name.
        buckets = p.buckets_list(bucket=name, verbosity="full")

        if buckets["total"] == 0:
            logger.error('Bucket not found.')
            sys.exit(1)

        bucket = buckets["data"][0]

    bucket_state = bucket["state"]["descriptor"]

    if bucket_state != "New":
        click.echo(f"Bucket state is \"{bucket_state}\" - only \"New.\" buckets can be completed.")
        sys.exit(1)

    click.echo(p.buckets_complete(bucket["id"]))


@click.command("errorFile")
@click.option("-n", "name",
              help="Bucket name.")
@click.argument("id", required=False)
@click.pass_context
def buckets_errorFile(ctx, name, id):
    """
    Return the error file for a bucket.

    [ID] A reference to a Prism Analytics bucket.
    """
    p = ctx.obj["p"]

    if id is not None:
        # If the caller passed both a name and WID, then use the WID first.
        error_file = p.buckets_errorFile(id=id)
    else:
        # Lookup the bucket by name.
        buckets = p.buckets_get(name=name)

        if buckets["total"] == 0:
            logger.error('Bucket not found.')
            sys.exit(1)

        error_file = p.buckets_errorFile(id=buckets['data'][0]['id'])

    logger.info(error_file)


@click.command("status")
@click.option("-n", "--name", required=False, help="Bucket name to status")
@click.argument("id", required=False)
@click.pass_context
def buckets_status(ctx, name, id):
    """
    Get the status of a bucket by ID or name.

    [ID] A reference to a Prism Analytics bucket.
    """
    p = ctx.obj["p"]

    if id is None and name is None:
        logger.error('Please specify the ID or name of a bucket.')
        sys.exit(1)

    if id is not None:
        bucket = p.buckets_get(id)

        if bucket is None:
            logger.error(f'Bucket {id} not found.')
            sys.exit(1)
    else:
        buckets = p.buckets_get(id, name=name)

        if buckets["total"] == 0:
            logger.error(f'Bucket name {name} not found.')
            sys.exit(1)

        bucket = buckets['data'][0]

    logger.info(bucket["state"]["descriptor"])


@click.command("name")
@click.pass_context
def buckets_name(ctx):
    """
    Generate a bucket name to use for other bucket operations.
    """
    click.echo(ctx.obj["p"].buckets_gen_name())
