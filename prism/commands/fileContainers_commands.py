import click
import sys

from . import util as u


@click.command("create")
@click.pass_context
def fileContainers_create(ctx):
    """
    Create a new fileContainers object returning the ID.
    """
    p = ctx.obj["p"]

    fileContainer = p.fileContainers_create()

    if fileContainer is not None:
        click.echo(fileContainer["id"])
    else:
        sys.exit(1)


@click.command("list", help="List the files for a file container.")
@click.argument("fileContainerID")
@click.pass_context
def filecontainers_list(ctx, filecontainerid):
    p = ctx.obj["p"]

    files = p.filecontainers_list(filecontainerid)

    click.echo(files)


@click.command("load")
@click.option("-f", "--fileContainerID", default=None, help="Target File container ID, default to a new container.")
@click.argument("file", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def filecontainers_load(ctx, filecontainerid, file):
    """
    Load one or more file into a file container.

    [FILE] one or more files to load.
    """
    p = ctx.obj["p"]

    fid = u.fileContainers_load(p, filecontainerid, file)

    if fid is None:
        click.echo("Error loading fileContainer.")
    else:
        # Return the file container ID to the command line.  If a
        # filecontainerID was passed, simply return that id.
        click.echo(fid)
