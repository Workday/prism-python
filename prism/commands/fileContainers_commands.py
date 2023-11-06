import click
import sys
import json
import logging

logger = logging.getLogger("prismCLI")


@click.command("create")
@click.pass_context
def fileContainers_create(ctx):
    """Create a new fileContainers object returning the ID."""

    p = ctx.obj["p"]

    file_container = p.fileContainers_create()

    if file_container is not None:
        logger.info(json.dumps(file_container, indent=2))
    else:
        logger.error("Error creating file container.")
        sys.exit(1)


@click.command("get")
@click.argument("id", required=True)
@click.pass_context
def fileContainers_get(ctx, id):
    """List the files in a file container.

    [ID] File container ID to list.
    """

    p = ctx.obj["p"]

    files_list = p.fileContainers_get(id)

    logger.info(json.dumps(files_list, indent=2))


@click.command("load")
@click.option(
    "-i", "--id", default=None, help="Target File container ID - defaults to a new container.",
)
@click.argument("file", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def fileContainers_load(ctx, id, file):
    """Load one or more files into a file container returning the container ID.

    [FILE] one or more CSV or GZipped CSV (.csv.gz) files to load.
    """

    if len(file) == 0:  # Click gives a tuple - even if no files included
        logger.error("One or more files must be specified.")
        sys.exit(1)

    p = ctx.obj["p"]

    # Load the file and retrieve the ID - a new fID is
    # created if the command line ID was not specified.
    # Subsequent files are loaded into the same container (fID).
    results = p.fileContainers_load(id=id, file=file)

    # If the fID comes back blank, then something is not
    # working.  Note: any error messages have already
    # been logged by the load operation.

    if results["total"] == 0:
        logger.error("A file container id is required to load a file.")
        sys.exit(1)
    else:
        # Return the file container ID to the command line.  If a
        # fileContainerID was passed, simply return that id.
        logger.info(json.dumps(results, indent=2))
