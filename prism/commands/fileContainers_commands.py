import click
import sys
import json

from . import util as u


@click.command("create")
@click.pass_context
def fileContainers_create(ctx):
    """Create a new fileContainers object returning the ID."""
    p = ctx.obj["p"]

    file_container = p.fileContainers_create()

    if file_container is not None:
        click.echo(file_container["id"])
    else:
        sys.exit(1)


@click.command("list")
@click.argument("fileContainerID")
@click.pass_context
def filecontainers_list(ctx, filecontainerid):
    """
    List the files in a file container.

    [fileContainerID] Container ID to list loaded files.
    """

    p = ctx.obj["p"]

    files = p.fileContainers_list(filecontainerid)

    click.echo(json.dumps(files,indent=2))


@click.command("load")
@click.option("-f", "--fileContainerID", default=None,
              help="Target File container ID - defaults to a new container.")
@click.argument("file", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def filecontainers_load(ctx, filecontainerid, file):
    """
    Load one or more files into a file container returning the container ID.

    [FILE] one or more CSV or GZipped CSV files to load.
    """

    if len(file) == 0:
        click.echo("One or more files must be specified.")

    p = ctx.obj["p"]

    # Load the file and retrieve the fID - a new fID is
    # created if the command line fID is not specified.
    # Subsequent files are loaded into the same container (fID).
    fid = p.fileContainers_load(filecontainerid, u.get_files(file))

    # If the fID comes back blank, then something is not
    # working.  Note: any error messages have already
    # been logged by the load operation.

    if fid is None:
        click.echo("Error loading fileContainer.")
        sys.exit(1)
    else:
        # Return the file container ID to the command line.  If a
        # fileContainerID was passed, simply return that id.
        click.echo(fid)
