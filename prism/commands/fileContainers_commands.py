import click
from commands import util as u


@click.command("create", help="Use this method to create a new fileContainers.")
@click.pass_context
def fileContainers_create(ctx):
    p = ctx.obj["p"]

    fileContainer = p.fileContainers_create()

    if fileContainer is not None:
        print(fileContainer["id"])
    else:
        print("")


@click.command("list", help="This resource returns all files for a file container.")
@click.argument("fileContainerID")
@click.pass_context
def filecontainers_list(ctx, filecontainerid):
    p = ctx.obj["p"]

    files = p.filecontainers_list(filecontainerid)

    print(files)


@click.command("load", help="This resource loads the file into a file container.")
@click.option("-f", "--fileContainerID", default=None, help="File container ID to load the file into.")
@click.argument("file", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def filecontainers_load(ctx, filecontainerid, file):
    p = ctx.obj["p"]

    fid = u.fileContainers_load_impl(p, filecontainerid, file)

    if fid is None:
        print("Error loading fileContainer.")
    else:
        # Return the file container ID to the command line.
        print(fid)
