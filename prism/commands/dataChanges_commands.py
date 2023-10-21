import sys
import click
import json
import logging
import time
import pandas as pd

logger = logging.getLogger('prismCLI')


@click.command("get")
@click.option("-n", "--isName", default=False, is_flag=True,
              help="Flag to treat the dct argument as a name.")
@click.option("-l", "--limit", default=-1,
              help="The maximum number of object data entries included in the response.")
@click.option("-o", "--offset", default=0,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-t", "--type", "type_", default="summary",
              help="How much information to be returned in response JSON (default=summary).")
@click.option("-f", "--format", "format_", default="full",
              help="Format output as full, summary, schema, or CSV.",
              type=click.Choice(['full', 'summary', 'schema', 'csv'], case_sensitive=False))
@click.option("-s", "--search", is_flag=True, default=False,
              help="Use contains search substring for --name or --id (default=false).")
@click.argument("dct", required=False)
@click.pass_context
def dataChanges_get(ctx, isname, dct, limit, offset, type_, format_, search):
    """View the data change tasks permitted by the security profile of the current user.

    [dct] A reference to a Prism Analytics Data Change Task.
    """
    p = ctx.obj["p"]

    def output_summary_dct(current_dct):
        """Display the one-line summary version of a DCT."""
        display_name = current_dct["displayName"]

        source_name = current_dct["source"]["sourceType"]
        source_name += ": " + current_dct["source"]["name"] if "name" in current_dct["source"] else ""

        target_name = current_dct["target"]["name"]
        operation = current_dct["operation"]["operationType"]["descriptor"]

        logger.info(f"{display_name}, source: {source_name}, target: {target_name}, operation: {operation}")

    # Separate the get calls because an ID lookup returns a dict and a name lookup
    # always returns an object/list structure with zero or more matching DCTs.
    if isname:
        data_change_task = p.dataChanges_get(name=dct, limit=limit, offset=offset, search=search, type=type_)

        if data_change_task["total"] == 0:
            logger.warning("No data change task(s) found.")
            sys.exit(1)

        # For display purposes, sort by display name (case-insensitive)
        data_change_task["data"] = sorted(
                                      data_change_task["data"],
                                      key=lambda dct_srt: dct_srt["displayName"].lower())
    else:
        data_change_task = p.dataChanges_get(id=dct, limit=limit, offset=offset, type_=type_)

        if data_change_task is None:
            logger.error(f'Data change task {dct} not found.')
            sys.exit(1)

    if format_ == "summary":
        if 'total' in data_change_task:
            for dct_item in data_change_task["data"]:
                output_summary_dct(dct_item)
        else:
            output_summary_dct(data_change_task)
    elif format_ == "csv":
        if 'data' in data_change_task:
            df = pd.json_normalize(data_change_task["data"])
        else:
            df = pd.json_normalize(data_change_task)

        logger.info(df.to_csv(index=False))
    else:
        if 'data' in data_change_task:
            logger.info(json.dumps(data_change_task["data"], indent=2))
        else:
            logger.info(json.dumps(data_change_task, indent=2))


@click.command("validate")
@click.option("-n", "--isName", default=False, is_flag=True,
              help="Flag to treat the dct argument as a name.")
@click.option("-s", "--search", is_flag=True,
              help="Use contains search substring for --name.")
@click.argument("dct", required=True)
@click.pass_context
def dataChanges_validate(ctx, isname, dct, search):
    """
    Validate the data change specified by name or ID.

    [DCT] A reference to a Prism Analytics Data Change Task.
    """

    p = ctx.obj["p"]

    if not isname:
        validate = p.dataChanges_validate(id)
        logger.info(json.dumps(validate, indent=2))
    else:
        data_change_tasks = p.dataChanges_get(name=dct, search=search)

        if data_change_tasks["total"] == 0:
            logger.error("No matching data change task(s) found.")
            sys.exit(1)

        results = []

        for dct in data_change_tasks["data"]:
            validate = p.dataChanges_validate(dct["id"])

            if 'error' in validate:
                # Add identifying attributes to the error message.
                validate['id'] = dct['id']
                validate['descriptor'] = dct['displayName']

            results.append(validate)

        logger.info(json.dumps(results, indent=2))


@click.command("run")
@click.option("-n", "--isName", default=False, is_flag=True,
              help="Flag to treat the dct argument as a name.")
@click.argument('dct', required=True)
@click.argument('fid', required=False)
@click.pass_context
def dataChanges_run(ctx, dct, fid, isname):
    """Execute the named data change task with an optional file container.

    [DCT]  A reference to a Prism Analytics data change.
    [FID]  An optional reference to a Prism Analytics file container.
    """

    p = ctx.obj["p"]

    if isname:
        # See if we have any matching data change task by name (with minor clean-up).
        data_changes = p.dataChanges_get(name=dct.replace(" ", "_"))

        if data_changes["total"] != 1:
            logger.error(f"Data change task not found: {dct}")
            sys.exit(1)

        dct_id = data_changes["data"][0]["id"]
        logger.debug(f'resolved ID: {dct_id}')
    else:
        dct_id = dct

    # It is valid to run a data change task without a fileContainerID value.
    activity = p.dataChanges_activities_post(dct_id, fid)

    if activity is None:
        logger.error("Failed to run data change task - please review the log.")
        sys.exit(1)

    # Output the results - could be the new activity id or an error message.
    logger.info(json.dumps(activity, indent=2))


@click.command("activities")
@click.option("-n", "--isName", default=False, is_flag=True,
              help="Flag to treat the dct argument as a name.")
@click.option("-s", "--status", is_flag=True, default=False,
              help="Return only the status of the activity.")
@click.argument("dct", required=True)
@click.argument("activityID", required=True)
@click.pass_context
def dataChanges_activities(ctx, dct, activityid, status, isname):
    """Get the status for a specific activity associated with a data change task.

    [DCT]        A reference to a data change task.
    [ACTIVITYID] A reference to a data change task activity.
    """

    p = ctx.obj["p"]

    if isname:
        # See if we have any matching data change task.
        data_changes = p.dataChanges_list(name=dct.replace(" ", "_"))

        if data_changes["total"] != 1:
            logger.error(f"Data change task not found: {dct}")
            sys.exit(1)

        dct_id = data_changes["data"][0]["id"]
        logger.debug(f'resolved ID: {dct_id}')
    else:
        dct_id = dct

    current_status = p.dataChanges_activities_get(dct_id, activityid)

    if current_status is None:
        logger.error("Activity for DCT not found.")
        sys.exit(1)
    else:
        if status:
            logger.info(current_status["state"]["descriptor"])
        else:
            logger.info(json.dumps(current_status, indent=2))


@click.command("upload")
@click.option("-n", "--isName", default=False, is_flag=True,
              help="Flag to treat the dct argument as a name.")
@click.option("-w", "--wait", default=False, is_flag=True,
              help="Wait for the data change task to complete.")
@click.option("-v", "--verbose", default=False, is_flag=True,
              help="Display additional information.")
@click.argument("dct", required=True)
@click.argument("file", required=True, nargs=-1, type=click.Path(exists=True))
@click.pass_context
def dataChanges_upload(ctx, isname, dct, file, wait, verbose):
    """Execute a data change task using the provided file(s).

    [DCT]  A reference to a Prism Analytics Data Change Task.
    [FILE] One or more .CSV or .CSV.GZ files.
    """

    p = ctx.obj["p"]

    if isname:
        data_change_tasks = p.dataChanges_get(name=dct)

        if data_change_tasks['total'] == 0:
            logger.error('Data change task not found.')
            sys.exit(1)

        dct_id = data_change_tasks['data'][0]['id']
        logger.debug(f'resolved ID: {dct_id}')
    else:
        dct_id = dct

    # Specifying None for the ID to create a new file container.
    file_container = p.fileContainers_load(id=None, file=file)

    if file_container['total'] == 0:
        logger.error('Error loading file container.')
        sys.exit(1)

    fid = file_container['id']
    logger.debug(f'new file container ID: {fid}')

    # Execute the DCT.
    activity = p.dataChanges_activities_post(id=dct_id, fileContainerID=fid)

    if 'errors' in activity:
        # Add the ID of the DCT for easy identification.
        activity['id'] = dct_id

        logger.error(json.dumps(activity, indent=2))

        sys.exit(1)

    if not wait:
        logger.info(json.dumps(activity, indent=2))
    else:
        activity_id = activity['id']

        while True:
            time.sleep(10)

            activity = p.dataChanges_activities_get(id=dct_id, activityID=activity_id)

            status = activity['state']['descriptor']

            if verbose:
                logger.info(f'Status: {status}')

            if status not in ['New', 'Queued', 'Processing', 'Loading']:
                break

        # Output the final status of the activity.
        logger.info(json.dumps(activity, indent=2))
