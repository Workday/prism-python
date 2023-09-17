import sys
import click
import json
import pandas as pd

@click.command("list")
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
@click.option("-f", "--format", "format_",
              default="full",
              help="Format output as full, summary, schema, or CSV.",
              type=click.Choice(['full', 'summary', 'schema', 'csv'], case_sensitive=False))
@click.option("-s", "--search", is_flag=True, help="Use contains search substring for --name or --id (default=false).")
@click.argument("name", required=False)
@click.pass_context
def dataChanges_list(ctx, name, wid, activity_wid, limit, offset, type_, format_, search):
    """
    View the data change tasks permitted by the security profile of the current user.

    [NAME] data change task to lists.
    """
    p = ctx.obj["p"]

    data_changes = p.dataChanges_list(name, wid, activity_wid, limit, offset, type_, search)

    if data_changes["total"] == 0:
        print("No data change tasks found.")
        return

    data_changes["data"] = sorted(data_changes["data"], key=lambda dct: dct["displayName"].lower())

    # Handle output
    if format_ == "summary":
        for dct in data_changes["data"]:
            display_name = dct["displayName"]

            source_name = dct["source"]["sourceType"]
            source_name += ": " + dct["source"]["name"] if "name" in dct["source"] else ""

            target_name = dct["target"]["name"]
            operation = dct["operation"]["operationType"]["descriptor"]

            print(f"{display_name}, source: {source_name}, target: {target_name}, operation: {operation}")
    elif format_ == "csv":
        df = pd.json_normalize(data_changes["data"])
        print(df.to_csv(index=False))
    else:
        print(json.dumps(data_changes["data"], indent=2))


@click.command("validate", help="Validate the data change specified by name or ID.")
@click.option("-w", "--wid", help="The dataChangeID to list.")
@click.option("-s", "--search", is_flag=True, help="Use contains search substring for --name or --id (default=false).")
@click.argument("name", required=False)
@click.pass_context
def dataChanges_validate(ctx, name, wid, search):
    p = ctx.obj["p"]

    if name is None and wid is None:
        print("A data change task name or a wid must be specified.")
        sys.exit(1)

    # See if we have any matching data change tasks.
    data_changes = p.dataChanges_list(
        name=name,
        wid=wid,
        search=search,
        refresh=True)

    if data_changes["total"] == 0:
        print("No matching data change task(s) found.")
        sys.exit(1)

    for dct in data_changes["data"]:
        validate = p.dataChanges_validate(dct["id"])
        print(validate)


@click.command("run")
@click.argument("name", required=True)
@click.argument("fileContainerID", required=False)
@click.pass_context
def dataChanges_run(ctx, name, filecontainerid):
    """
    This resource executes a data change.

    [NAME]  Data Change Task name.
    [FILECONTAINERID] File container with files to load.
    """

    p = ctx.obj["p"]

    # See if we have any matching data change task.
    data_changes = p.dataChanges_list(name=name.replace(" ", "_"), type_="full", refresh=True)

    if data_changes["total"] != 1:
        print(f"Data change task not found: {name}")
        sys.exit(1)

    dct_id = data_changes["data"][0]["id"]

    validate = p.dataChanges_validate(dct_id)

    if "error" in validate:
        print("Invalid DCT: " + validate["errors"][0]["error"] + " - code: " + validate["errors"][0]["code"])
        sys.exit(1)
    else:
        activity_id = p.dataChanges_activities_post(dct_id, filecontainerid)

        if activity_id is None:
            sys.exit(1)
        else:
            print(activity_id)


@click.command("activities")
@click.option("-s", "--status", is_flag=True, default=False,
              help="Return only the status of the activity.")
@click.argument("name", required=True)
@click.argument("activity_id", required=True)
@click.pass_context
def dataChanges_activities(ctx, status, name, activity_id):
    """
    This resource executes a data change.

    [NAME]  Data Change Task name.
    [FILECONTAINERID] File container with files to load.
    """

    p = ctx.obj["p"]

    # See if we have any matching data change task.
    data_changes = p.dataChanges_list(name=name.replace(" ", "_"), type_="full", refresh=True)

    if data_changes["total"] != 1:
        print(f"Data change task not found: {name}")
        sys.exit(1)

    dct_id = data_changes["data"][0]["id"]

    current_status = p.dataChanges_activities_get(dct_id, activity_id)

    if current_status is None:
        sys.exit(1)
    else:
        if status:
            print(current_status["state"]["descriptor"])
        else:
            print(current_status)