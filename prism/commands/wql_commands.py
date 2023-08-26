import click
import json
import sys


@click.command("dataSources",
               help="View the buckets permitted by the security profile of the current user.")
@click.option("-w", "--wid",
              help="The Workday ID of the dataSources.")
@click.option("-l", "--limit", default=None,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-s", "--search", is_flag=True, show_default=True, default=False,
              help="Use contains search substring for --table_name or --id.")
@click.argument("name", required=False)
@click.pass_context
def dataSources(ctx, wid, limit, offset, search, name):
    p = ctx.obj["p"]

    ds = p.wql_dataSources(wid, limit, offset, name, search)

    print(json.dumps(ds, indent=2))


@click.command("data",
               help="View the buckets permitted by the security profile of the current user.")
@click.option("-l", "--limit", default=None,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-f", "--file", "file_", default=None, type=click.Path(exists=True),
              help="Filename of a query")
@click.option("-c", "--csv", "csv_", is_flag=True, show_default=True, default=False,
              help="Output query results as CSV.")
@click.argument("query", required=False)
@click.pass_context
def data(ctx, limit, offset, file_, csv_, query):
    p = ctx.obj["p"]

    if file_ is None and query is None:
        print("No query provided")
        return

    if query is not None:
        query_resolved = query
    else:
        with open(file_) as file:
            query_resolved = file.read().replace('\n',' ')

    rows = p.wql_data(query_resolved, limit, offset)

    if rows["total"] != 0:
        if csv_:
            headers = rows["data"][0].keys()

            writer = csv.DictWriter(sys.stdout, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows["data"])
        else:
            print(json.dumps(data, indent=2))

