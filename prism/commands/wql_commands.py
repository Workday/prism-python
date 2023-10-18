import click
import json
import sys
import pandas as pd


@click.command("dataSources")
@click.option("-w", "--wid",
              help="The Workday ID of the data source.")
@click.option("-l", "--limit", default=None,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-s", "--search", is_flag=True, show_default=True, default=False,
              help="Use contains search substring for --table_name or --id.")
@click.argument("name", required=False)
@click.pass_context
def dataSources(ctx, wid, limit, offset, search, name):
    """Returns a collection of data sources (/dataSources) for use in a WQL query."""
    p = ctx.obj["p"]

    ds = p.wql_dataSources(wid, limit, offset, name, search)

    click.echo(json.dumps(ds, indent=2))


@click.command("data")
@click.option("-l", "--limit", default=None,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-f", "--file", "wql_file", default=None, type=click.Path(exists=True),
              help="Filename containing a WQL query.")
@click.option("-c", "--as_csv", "as_csv", is_flag=True, show_default=True, default=False,
              help="Output query results as CSV.")
@click.argument("query", required=False)
@click.pass_context
def data(ctx, limit, offset, wql_file, as_csv, query):
    """
    Returns the data from a WQL query.

    [QUERY] WQL query string to execute (/data).
    """
    p = ctx.obj["p"]

    if wql_file is None and query is None:
        click.echo("No query provided")
        sys.exit(1)

    if query is not None:
        query_resolved = query
    else:
        with open(wql_file) as file:
            query_resolved = file.read().replace('\n', ' ')

    rows = p.wql_data(query_resolved, limit, offset)

    if rows["total"] != 0:
        if as_csv:
            df = pd.json_normalize(rows["data"])
            click.echo(df.to_csv(index=False))
        else:
            click.echo(json.dumps(rows, indent=2))
