import sys
import os
import logging
import click
import json
import pandas as pd

from . import util as u

logger = logging.getLogger("prismCLI")


@click.command("list")
@click.option("-w", "--wid",
              help="Unique WID for Prism table or dataset.")
@click.option("-l", "--limit", type=int, default=None,
              help="The maximum number of object data entries included in the response, default=all.")
@click.option("-o", "--offset", type=int, default=None,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-t", "--type", "type_", default="summary",
              type=click.Choice(["summary", "full", "permissions"], case_sensitive=False),
              help="How much information returned for each table.")
@click.option("-f", "--format", "format_", default="json",
              type=click.Choice(['json', 'summary', 'schema', 'csv'], case_sensitive=False),
              help="Format output as JSON, summary, schema, or CSV.")
@click.option("-s", "--search", is_flag=True,
              help="Enable substring search of NAME in api name or display name, default=False (exact match).")
@click.argument("name", required=False)
@click.pass_context
def tables_list(ctx, name, wid, limit, offset, type_, format_, search):
    """List the tables or datasets permitted by the security profile of the current user.

    [NAME] Prism table name to list.
    """

    if type_ in ("summary", "permissions") and format_ in ("schema", "csv"):
        # Summary results cannot generate schema or CSV output since there will be no fields.
        logger.critical(f"Invalid combination of type \"{type_}\" and format \"{format_}\".")
        sys.exit(1)

    p = ctx.obj["p"]

    # Query the tenant...
    tables = p.tables_list(name, wid, limit, offset, type_, search)

    # The return always has a total tables returned value.
    # note: tables_list never fails, it simply returns 0 tables if there is a problem.
    if tables["total"] == 0:
        return

    # Handle output
    if format_ == "json":
        # The results could be one table or an array of multiple
        # tables - simply dump the returned object.

        click.echo(json.dumps(tables, indent=2))
    elif format_ == "summary":
        for table in tables["data"]:
            display_name = table["displayName"]
            rows = table["stats"]["rows"] if "stats" in table and "rows" in table["stats"] else "Null"
            size = table["stats"]["size"] if "stats" in table and "size" in table["stats"] else "Null"
            refreshed = table["dateRefreshed"] if "dateRefreshed" in table else "unknown"
            enabled = table["enableForAnalysis"] if "enableForAnalysis" in table else "Null"

            click.echo(f'{display_name}, Enabled: {enabled}, Rows: {rows}, Size: {size}, Refreshed: {refreshed}')
    elif format_ == "csv":
        df = pd.json_normalize(tables["data"])
        click.echo(df.to_csv(index=False))
    elif format_ == "schema":
        # Dump out the fields of the first table in the result in
        # a format compatible with a schema used to created or edit
        # a table.

        table = tables["data"][0]  # Only output the first table.

        # Remove the Prism audit columns.
        fields = [fld for fld in tables["data"][0]["fields"] if not fld["name"].startswith("WPA_")]

        # Remove tenant specific values - these are not needed
        # if the user wants to update a table definition.

        for fld in fields:
            if "fieldId" in fld:
                if "fieldId" in fld:
                    del fld["fieldId"]

                if "id" in fld:
                    del fld["id"]

        click.echo(json.dumps(fields, indent=2))
    else:
        click.echo(u.csv_from_fields(tables["data"][0]["fields"]))


@click.command("create")
@click.option("-n", "--name",
              help="Table name - overrides name from schema.")
@click.option("-d", "--displayName",
              help="Specify a display name - defaults to name.")
@click.option("-t", "--tags", multiple=True,
              help="Tags to organize the table in the Data Catalog.")
@click.option("-e", "--enableForAnalysis", type=bool, is_flag=True, default=None,
              help="Enable this table for analytics.")
@click.option("-s", "--sourceName",
              help="The API name of an existing table to copy.")
@click.option("-w", "--sourceWID",
              help="The WID of an existing table to copy.")
@click.argument("file", required=False, type=click.Path(exists=True))
@click.pass_context
def tables_create(ctx, name, displayname, tags, enableforanalysis, sourcename, sourcewid, file):
    """
    Create a new table with the specified name.

    [FILE] Optional file containing a schema definition for the table.

    Note: A schema file, --sourceName, or --sourceWID must be specified.
    """
    p = ctx.obj["p"]

    # We can assume a valid schema - get_schema sys.exits if there is a problem.
    schema = u.get_schema(p, file, sourcename, sourcewid)

    # Initialize a new schema with the particulars for this table operation.
    if name is not None:
        # If we got a name, set it in the table schema
        schema["name"] = name.replace(" ", "_")  # Minor clean-up
    elif "name" not in schema:
        # The schema doesn't have a name and none was given - exit.
        logger.critical("Table --name must be specified.")
        sys.exit(1)

    if displayname is not None:
        # If we got a display name, set it in the schema
        schema["displayName"] = displayname
    elif "displayName" not in schema:
        # Default the display name to the name if not in the schema.
        schema["displayName"] = name

    if enableforanalysis is not None:
        schema["enableForAnalysis"] = enableforanalysis
    elif "enableForAnalysis" not in schema:
        # Default to False - do not enable.
        schema["enableForAnalysis"] = False

    # Create the table.
    table_def = p.tables_create(schema)

    if table_def is not None:
        click.echo(f"Table {name} created.")
    else:
        click.echo(f"Error creating table {name}.")


@click.command("update")
@click.option("-s", "--sourceName", help="The API name of an existing table to copy.")
@click.option("-w", "--sourceWID", help="The ID of an existing table to copy.")
@click.option("-t", "--truncate", is_flag=True, default=False, help="Truncate the table before updating.")
@click.argument("name", required=True)
@click.argument("file", required=False, type=click.Path(exists=True))
@click.pass_context
def tables_update(ctx, name, file, sourcename, sourcewid, truncate):
    """Edit the schema for an existing table.

    NAME   The API name of the table to update\b
    [FILE] Optional file containing an updated schema definition for the table.

    Note: A schema file, --sourceName, or --sourceWID must be specified.
    """

    p = ctx.obj["p"]

    # Before doing anything, table name must exist.
    tables = p.tables_list(name=name)

    if tables["total"] == 0:
        logger.critical(f"Table \"{name}\" not found.")
        sys.exit(1)

    table_id = tables["data"][0]["id"]

    # Figure out the new schema either by file or other table.
    fields = u.get_schema(p, file, sourcename, sourcewid)

    p.tables_update(wid=table_id, schema=file, truncate=truncate)

    click.echo("update")


@click.command("upload")
@click.option("-o", "--operation", default="TruncateAndInsert",
              help="Operation for the table operation - default to TruncateAndInsert.")
@click.argument("name", required=True)
@click.argument("file", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def tables_upload(ctx, name, operation, file):
    """
    Upload a file into the table using a bucket.

    NOTE: This operation creates ".csv.gz" files for each .csv file.
    """
    p = ctx.obj["p"]

    # Convert the file(s) provided to a list of compressed files.
    target_files = u.get_files(file)

    if len(target_files) == 0:
        logging.getLogger("prismCLI").critical("No files to upload.")
        sys.exit(1)

    bucket = p.buckets_create(target_name=name, operation=operation)

    if bucket is None:
        logging.getLogger("prismCLI").critical("Bucket creation failed.")
        sys.exit(1)

    results = p.buckets_upload(bucket["id"], target_files)

    if len(results) > 0:
        p.buckets_complete(bucket["id"])


@click.command("truncate")
@click.argument("name", required=True)
@click.pass_context
def tables_truncate(ctx, name):
    """
    Truncate the named table.

    :param name:
    :return:
    """
    # Create an empty bucket with a delete operation
    p = ctx.obj["p"]

    # Get a bucket using a generated name and an explicit Delete operation.
    bucket = p.buckets_create(target_name=name, operation="TruncateAndInsert")

    if bucket is None:
        logging.getLogger("prismCLI").critical(f"Unable to truncate {name} - error getting bucket.")
        sys.exit(1)

    bucket_id = bucket["id"]

    # Don't specify a file to put a zero sized file into the bucket.
    bucket = p.buckets_upload(bucket_id)

    # Ask Prism to run the delete statement by completing the bucket.
    bucket = p.buckets_complete(bucket_id)

    if bucket is None:
        click.echo(f"Unable to truncate table {name}.")
