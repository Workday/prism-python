import click
import json
import prism


@click.group()
@click.option("--base_url", envvar="workday_base_url", type=str, required=True, help="The base URL for the API client")
@click.option(
    "--tenant_name", envvar="workday_tenant_name", type=str, required=True, help="The name of your Workday tenant"
)
@click.option(
    "--client_id",
    envvar="prism_client_id",
    type=str,
    required=True,
    help="The client ID for your registered API client",
)
@click.option(
    "--client_secret",
    envvar="prism_client_secret",
    type=str,
    required=True,
    help="The client secret for your registered API client",
)
@click.option(
    "--refresh_token",
    envvar="prism_refresh_token",
    type=str,
    required=True,
    help="The refresh token for your registered API client",
)
@click.pass_context
def main(ctx, base_url, tenant_name, client_id, client_secret, refresh_token):
    """CLI for interacting with Workday’s Prism API"""

    # initialize the prism class with your credentials
    p = prism.Prism(base_url, tenant_name, client_id, client_secret, refresh_token, version="v2")

    # create the bearer token
    p.create_bearer_token()

    # store the prism object in the context
    ctx.obj = {"p": p}


@main.command()
@click.option("--name", default=None, type=str, help="The name of the table to obtain details about")
@click.pass_context
def list(ctx, name):
    """List all tables of type API"""

    # get the initialized prism class
    p = ctx.obj["p"]

    # list the tables
    status = p.list_table(table_name=name)

    # print message
    if id is None:
        click.echo("There are {} API tables".format(status["total"]))
        click.echo(json.dumps(status["data"], indent=2, sort_keys=True))
    else:
        click.echo(json.dumps(status, indent=2, sort_keys=True))


@main.command()
@click.option(
    "--table_name",
    type=str,
    required=True,
    help="The table name. The name must be unique and conform to the name validation rules",
)
@click.option("--schema_path", type=click.Path(), required=True, help="The path to your schema file")
@click.pass_context
def create(ctx, table_name, schema_path):
    """Create a new Prism table"""

    # get the initialized prism class
    p = ctx.obj["p"]

    # read in your table schema
    schema = prism.load_schema(schema_path)

    # clean up the table name
    table_name = table_name.replace(" ", "_")

    # create an empty API table
    table = p.create_table(table_name, schema=schema["fields"])

    # print message
    click.echo(json.dumps(table, indent=2, sort_keys=True))


@main.command()
@click.option("--table_id", type=str, required=True, help="The Table ID to upload your file to")
@click.option("--data_path", type=click.Path(), required=True, help="The path to your gzip compressed data file")
@click.option(
    "--operation",
    type=click.Choice(["TruncateandInsert", "Insert", "Update", "Upsert", "Delete"]),
    default="TruncateandInsert",
    help="The Table load operation",
)
@click.pass_context
def upload(ctx, table_id, data_path, operation):
    """Upload a gzip CSV file"""

    # get the initialized prism class
    p = ctx.obj["p"]

    # get the details about the newly created table
    details = p.describe_table(table_id)

    # convert the details to a bucket schema
    bucket_schema = p.convert_describe_schema_to_bucket_schema(details)

    # create a new bucket to hold your file
    bucket = p.create_bucket(bucket_schema, table_id, operation=operation)

    # add your file to the bucket you just created
    p.upload_file_to_bucket(bucket["id"], data_path)

    # complete the bucket and upload your file
    p.complete_bucket(bucket["id"])

    # check the status of the table you just created
    status = p.list_table(table_id)

    # print message
    click.echo(json.dumps(status["data"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
