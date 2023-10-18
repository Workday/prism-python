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

    # Regardless of success, the list operation always returns
    # a valid object.  Error messages will appear in the log.
    if data_changes["total"] == 0:
        click.echo("No data change tasks found.")
        return

    # For display purposes, sort by display name (case-insensitive)
    data_changes["data"] = sorted(data_changes["data"], key=lambda dct: dct["displayName"].lower())

    # Handle output
    if format_ == "summary":
        for dct in data_changes["data"]:
            display_name = dct["displayName"]

            source_name = dct["source"]["sourceType"]
            source_name += ": " + dct["source"]["name"] if "name" in dct["source"] else ""

            target_name = dct["target"]["name"]
            operation = dct["operation"]["operationType"]["descriptor"]

            click.echo(f"{display_name}, source: {source_name}, target: {target_name}, operation: {operation}")
    elif format_ == "csv":
        df = pd.json_normalize(data_changes["data"])
        click.echo(df.to_csv(index=False))
    else:
        click.echo(json.dumps(data_changes["data"], indent=2))


@click.command("validate")
@click.option("-w", "--wid", help="The dataChangeID to list.")
@click.option("-s", "--search", is_flag=True, help="Use contains search substring for --name or --id (default=false).")
@click.argument("name", required=False)
@click.pass_context
def dataChanges_validate(ctx, name, wid, search):
    """
    Validate the data change specified by name or ID.

    [NAME] The API name of the data change task to validate
    """

    p = ctx.obj["p"]

    if name is None and wid is None:
        click.echo("A data change task name or wid must be specified.")
        sys.exit(1)

    # See if we have any matching data change tasks.
    # Note: datachanges_list never fails - errors may appear in the log
    data_changes = p.dataChanges_list(
        name=name,
        wid=wid,
        search=search,
        refresh=True)

    if data_changes["total"] == 0:
        click.echo("No matching data change task(s) found.")
    else:
        for dct in data_changes["data"]:
            validate = p.dataChanges_validate(dct["id"])
            click.echo(validate)


@click.command("run")
@click.argument("name", required=True)
@click.argument("fileContainerID", required=False)
@click.pass_context
def dataChanges_run(ctx, name, filecontainerid):
    """
    Execute the named data change task with an optional file container.

    [NAME]  Data Change Task name.
    [FILECONTAINERID] File container with files to load.
    """

    p = ctx.obj["p"]

    # See if we have any matching data change task.
    data_changes = p.dataChanges_list(name=name.replace(" ", "_"), type_="full", refresh=True)

    if data_changes["total"] != 1:
        click.echo(f"Data change task not found: {name}")
        sys.exit(1)

    dct_id = data_changes["data"][0]["id"]

    validate = p.dataChanges_validate(dct_id)

    if "error" in validate:
        click.echo("Invalid DCT: " + validate["errors"][0]["error"] + " - code: " + validate["errors"][0]["code"])
        sys.exit(1)
    else:
        # It is valid to run a data change task without a fileContainerID value.
        activity_id = p.dataChanges_activities_post(dct_id, filecontainerid)

        if activity_id is None:
            click.echo("Failed to run data change task - please review the log.")
            sys.exit(1)
        else:
            click.echo(activity_id)


@click.command("activities")
@click.option("-s", "--status", is_flag=True, default=False,
              help="Return only the status of the activity.")
@click.argument("name", required=True)
@click.argument("activity_id", required=True)
@click.pass_context
def dataChanges_activities(ctx, status, name, activity_id):
    """
    Get the status for a specific activity associated with a data change task.

    [NAME]  Data Change Task name.
    [ACTIVITY_ID] File container with files to load.
    """

    p = ctx.obj["p"]

    # See if we have any matching data change task.
    data_changes = p.dataChanges_list(name=name.replace(" ", "_"), type_="full", refresh=True)

    if data_changes["total"] != 1:
        click.echo(f"Data change task not found: {name}")
        sys.exit(1)

    dct_id = data_changes["data"][0]["id"]

    current_status = p.dataChanges_activities_get(dct_id, activity_id)

    if current_status is None:
        click.echo("Activity for DCT not found.")
        sys.exit(1)
    else:
        if status:
            click.echo(current_status["state"]["descriptor"])
        else:
            click.echo(current_status)