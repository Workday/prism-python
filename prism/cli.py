import click
import configparser
import os
import sys

import prism

from commands import tables_commands
from commands import buckets_commands
from commands import dataChanges_commands
from commands import fileContainers_commands
from commands import wql_commands
from commands import raas_commands


def param_fixup(value, config, config_name, option):
    # If already set by an enviroment or by a command line option, do nothing.
    if value is not None:
        return value

    try:
        return config.get(config_name, option)
    except configparser.Error:
        # Always fail silently.
        return None


@click.group(help="CLI for interacting with Workday’s Prism API")
@click.option(
    "--base_url",
    envvar="workday_base_url",
    type=str,
    required=False,
    help="The base URL for the API client")
@click.option(
    "--tenant_name",
    envvar="workday_tenant_name",
    type=str,
    required=False,
    help="The name of your Workday tenant")
@click.option(
    "--username",
    envvar="workday_username",
    type=str,
    required=False,
    help="The login username of your Workday user")
@click.option(
    "--password",
    envvar="workday_password",
    type=str,
    required=False,
    help="The password of your Workday user")
@click.option(
    "--client_id",
    envvar="prism_client_id",
    type=str,
    required=False,
    help="The client ID for your registered API client")
@click.option(
    "--client_secret",
    envvar="prism_client_secret",
    type=str,
    required=False,
    help="The client secret for your registered API client")
@click.option(
    "--refresh_token",
    envvar="prism_refresh_token",
    type=str,
    required=False,
    help="The refresh token for your registered API client")
@click.option(
    "--log_level",
    envvar="prism_log_level",
    type=str,
    required=False,
    help="Level of debugging to display - default = warning.")
@click.option(
    "--log_file",
    envvar="prism_log_file",
    type=str,
    required=False,
    help="Output file for logging - default prism.log.")
@click.option(
    "--config_file",
    envvar="prism_config",
    type=click.Path(exists=True),
    required=False,
    help="The name of a configuration with parameters for connections and logging.")
@click.option(
    "--config_name",
    envvar="prism_config",
    type=str,
    required=False,
    default="default",
    help="The name of a configuration with parameters for connections and logging.")
@click.pass_context
def cli(ctx,
        base_url, tenant_name,
        username, password,
        client_id, client_secret, refresh_token,
        log_level, log_file,
        config_file, config_name):
    # Attempt to locate a configuration file - this is not required and is only
    # used if the configuration values are not passed on the command line or in
    # the environment.

    if config_file is None:
        # See if we have a configuration file in the current directory
        filename = os.path.join(os.getcwd(), "prism.ini")
    else:
        filename = config_file

    # If the configuration path exists, then load values - this overrides
    # environment variables.
    if os.path.isfile(filename):
        try:
            config = configparser.ConfigParser()
            config.read(filename)

            # Check to see if a particular configuration was asked for, it must
            # exist in the configuration file otherwise exit with an error.

            if not config.has_section(config_name):
                print(f"The specified configuration {config_name} does not exist in the configuration file.")
                sys.exit(1)
            else:
                # Do fix-up on command line args.  Priority comes from the command
                # line, then environment variables, and finally the config file.
                # Any value not passed and not in the environment arrives here with
                # the value "None" - override these with the configuration values.

                base_url = param_fixup(base_url, config, config_name, "workday_base_url")
                tenant_name = param_fixup(tenant_name, config, config_name, "workday_tenant_name")
                username = param_fixup(username, config, config_name, "workday_username")
                password = param_fixup(password, config, config_name, "workday_password")
                client_id = param_fixup(client_id, config, config_name, "prism_client_id")
                client_secret = param_fixup(client_secret, config, config_name, "prism_client_secret")
                refresh_token = param_fixup(refresh_token, config, config_name, "prism_refresh_token")
                log_level = param_fixup(log_level, config, config_name, "prism_log_level")
                log_file = param_fixup(log_level, config, config_name, "prism_log_file")
        except configparser.Error:
            print(f"Error accessing configuration file {filename}.")
            # If the configuration is not available, exit
            exit(1)

    if log_file is None:
        log_file = "prism.log"

    # initialize the prism class with your credentials

    p = prism.Prism(base_url, tenant_name, client_id, client_secret, refresh_token)
    p.set_log_level(log_level)

    # store the prism object in the context
    ctx.obj = {"p": p}


@cli.command("config")
@click.argument("file")
@click.pass_context
def config_file(ctx, file):
    """Configure command"""


@cli.group("tables", help="Commands to list, create, load, and update Prism tables.")
def tables():
    """Tables Command"""


tables.add_command(tables_commands.tables_list)
tables.add_command(tables_commands.tables_create)
tables.add_command(tables_commands.tables_update)
tables.add_command(tables_commands.tables_upload)
tables.add_command(tables_commands.tables_truncate)


@cli.group("buckets", help="Bucket operations to list, create and load buckets.")
def buckets():
    """You create a bucket for a specific table, load data into the bucket, and then commit (complete) the bucket. """


buckets.add_command(buckets_commands.buckets_list)
buckets.add_command(buckets_commands.buckets_create)
buckets.add_command(buckets_commands.buckets_complete)
buckets.add_command(buckets_commands.buckets_status)
buckets.add_command(buckets_commands.buckets_upload)
buckets.add_command(buckets_commands.buckets_name)


@cli.group("dataChanges", help="Data Change Tasks (dataChanges) operations to list, load, and activate.")
def dataChanges():
    """dataChanges.py Command"""


dataChanges.add_command(dataChanges_commands.dataChanges_list)
dataChanges.add_command(dataChanges_commands.dataChanges_validate)
dataChanges.add_command(dataChanges_commands.dataChanges_run)
dataChanges.add_command(dataChanges_commands.dataChanges_activities)


@cli.group("fileContainers", help="File containers (fileContainers) operations to create, load, and list.")
def fileContainers():
    """dataChanges.py Command"""


fileContainers.add_command(fileContainers_commands.fileContainers_create)
fileContainers.add_command(fileContainers_commands.filecontainers_list)
fileContainers.add_command(fileContainers_commands.filecontainers_load)


@cli.group("wql", help="Operations to list (dataSources) and query WQL sources (data).")
def wql():
    """dataChanges.py Command"""


wql.add_command(wql_commands.dataSources)
wql.add_command(wql_commands.data)


@cli.group("raas", help="Run custom or Workday delivered report.")
def raas():
    """dataChanges.py Command"""


raas.add_command(raas_commands.run)


if __name__ == "__main__":
    cli()
