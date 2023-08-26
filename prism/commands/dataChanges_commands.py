import click
import pandas as pd

@click.command("list",
               help="View the data change tasks permitted by the security profile of the current user.")
@click.option("-w", "--wid",
              help="The dataChangeID to list.")
@click.option("-a", "--activity_wid",
              help="A specific activity associated with the data change task.")
@click.option("-l", "--limit", default=-1,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=0,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-t", "--type", "type_", default="summary",
              help="How much information to be returned in response JSON (default=summary).")
@click.option("-f", "--format",
              default="full",
              help="Format output as full, summary, schema, or CSV.",
              type=click.Choice(['full', 'summary', 'schema', 'csv'], case_sensitive=False))
@click.option("-s", "--search", is_flag=True, help="Use contains search substring for --name or --id (default=false).")
@click.argument("api_name", required=False)
@click.pass_context
def dataChanges_list(ctx, api_name, wid, activity_wid, limit, offset, type_, format, search):
    p = ctx.obj["p"]
    o = ctx.obj["o"]

    o.dataChanges_query()

    dataChanges = p.dataChanges_list(api_name, wid, activity_wid, limit, offset, type_, search)
    dataChanges["data"] = sorted(dataChanges["data"], key=lambda dct: dct["displayName"].lower())

    # Handle output
    for dct in dataChanges["data"]:
        print(dct["displayName"])


@click.command("validate", help="Validate the data change specified by name or ID.")
@click.option("-w", "--wid", help="The dataChangeID to list.")
@click.option("-s", "--search", is_flag=True, help="Use contains search substring for --name or --id (default=false).")
@click.argument("api_name", required=False)
@click.pass_context
def dataChanges_validate(ctx, api_name, wid, search):
    p = ctx.obj["p"]

    # See if we have any matching data change tasks.
    dataChanges = p.dataChanges_list(
        name=api_name,
        wid=wid,
        search=search,
        refresh=True)

    if dataChanges["total"] == 0:
        print("No matching data change task(s) found.")

    if len(dataChanges) == 1:
        print(p.dataChanges.activities_post(dataChanges["id"]))


@click.command("execute", help="This resource executes a data change.")
@click.argument("api_name", required=True)
@click.argument("fileContainerID", required=False)
@click.pass_context
def dataChanges_execute(ctx, api_name, filecontainerid):
    p = ctx.obj["p"]

    # See if we have any matching data change tasks.
    # See if we have any matching data change tasks.
    dataChanges = p.dataChanges_list(
        name=api_name,
        refresh=True)

    if dataChanges["total"] != 1:
        print("Invalid data change task to execute")
        return

    dct_id = dataChanges["data"][0]["id"]

    dataChanges = p.dataChanges_validate(dct_id)

    print(p.dataChanges_activities_post(dct_id, filecontainerid))
