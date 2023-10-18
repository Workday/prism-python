import click
import configparser
import os
import sys
import logging

import prism

import commands.tables_commands as t_commands
import commands.buckets_commands as b_commands
import commands.dataChanges_commands as d_commands
import commands.fileContainers_commands as f_commands
import commands.wql_commands as w_commands
import commands.raas_commands as r_commands


def param_fixup(value, config, config_name, option):
    # If already set by an environment or by a command line option, do nothing.
    if value is not None:
        return value

    try:
        return config.get(config_name, option)
    except configparser.Error:
        # Always fail silently.
        return None


@click.group(help="CLI for interacting with Workday’s Prism API")
# Tenant specific parameters
@click.option("--base_url", envvar="workday_base_url", type=str, required=False,
              help="The base URL for the API client")
@click.option("--tenant_name", envvar="workday_tenant_name", type=str, required=False,
              help="The name of your Workday tenant")
# Credentials parameters
@click.option("--username", envvar="workday_username", type=str, required=False,
              help="The login username of your Workday user")
@click.option("--password", envvar="workday_password", type=str, required=False,
              help="The password of your Workday user")
@click.option("--client_id", envvar="prism_client_id", type=str, required=False,
              help="The client ID for your registered API client")
@click.option("--client_secret", envvar="prism_client_secret", type=str, required=False,
              help="The client secret for your registered API client")
@click.option("--refresh_token", envvar="prism_refresh_token", type=str, required=False,
              help="The refresh token for your registered API client")
# Operational parameters
@click.option("--log_level", envvar="prism_log_level", type=str, required=False,
              help="Level of debugging to display - default = INFO")
@click.option("--log_file", envvar="prism_log_file", type=str, required=False,
              help="Output file for logging - default = prism.log")
@click.option("--config_file", envvar="prism_config_file", type=click.Path(exists=True), required=False,
              help="The name of a configuration with parameters for connections and logging.")
@click.option("--config_name", envvar="prism_config_name", type=str, required=False, default="default",
              help="The name of a configuration in the configuration file.")
@click.pass_context
def cli(ctx,
        base_url, tenant_name,
        username, password, client_id, client_secret, refresh_token,
        log_level, log_file,
        config_file, config_name):
    # Attempt to locate a configuration file - this is not required and config
    # parameters are only used if the configuration values are not passed on
    # the command line or by environment variables.

    if config_file is None:
        # Assume there might be a configuration file in the current directory
        filename = os.path.join(os.getcwd(), "prism.ini")
    else:
        # Click already ensured this is a valid file - if specified.
        filename = config_file

    # If the configuration path exists, then load values - this overrides
    # environment variables.
    if os.path.isfile(filename):
        try:
            config = configparser.ConfigParser()
            config.read(filename)

            # Check to see if a particular configuration [name] was asked for, it must
            # exist in the configuration file otherwise exit with an error.

            if config.has_section(config_name):
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
                log_file = param_fixup(log_file, config, config_name, "prism_log_file")
            else:
                click.echo(f"The specified configuration [{config_name}] does not exist in the configuration file.")
                sys.exit(1)
        except configparser.Error:
            click.echo(f"Error accessing configuration file {filename}.")
            # If the configuration is not available or is invalid, exit
            sys.exit(1)

    if log_level is None:
        set_level = logging.INFO
    else:
        set_level = getattr(logging, log_level)

    # Setup logging for CLI operations.
    logger = logging.getLogger('prismCLI')
    logger.setLevel(set_level)

    # Create a handler as specified by the user (or defaults)

    if log_file is not None:
        fh = logging.FileHandler(log_file)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        fh.setLevel(set_level)
        logger.addHandler(fh)

    # Create an explicit console handler with a higher log level
    ch = logging.StreamHandler()

    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)

    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    logger.debug("completed initialization.")

    # initialize the Prism class from our resolved configuration.

    p = prism.Prism(base_url, tenant_name, client_id, client_secret, refresh_token)
    prism.set_logging(log_file, log_level)

    # store the prism object in the Click context
    ctx.obj = {"p": p}


@cli.command("config")
@click.argument("file")
@click.pass_context
def config(ctx, file):
    """
    Configuration operations to list, create, and modify parameters
    """

    # TBD


@cli.group("tables")
def tables():
    """
    Table operations (/tables) to list, create, load, update, and truncate Prism tables.
    """


tables.add_command(t_commands.tables_list)
tables.add_command(t_commands.tables_create)
tables.add_command(t_commands.tables_update)
tables.add_command(t_commands.tables_upload)
tables.add_command(t_commands.tables_truncate)


@cli.group("buckets")
def buckets():
    """
    Bucket operations (/buckets) to list, create and load buckets.
    """


buckets.add_command(b_commands.buckets_list)
buckets.add_command(b_commands.buckets_create)
buckets.add_command(b_commands.buckets_complete)
buckets.add_command(b_commands.buckets_status)
buckets.add_command(b_commands.buckets_upload)
buckets.add_command(b_commands.buckets_name)


@cli.group("dataChanges")
def dataChanges():
    """
    Data Change Tasks (/dataChanges) operations to list, load, and activate.
    """


dataChanges.add_command(d_commands.dataChanges_list)
dataChanges.add_command(d_commands.dataChanges_validate)
dataChanges.add_command(d_commands.dataChanges_run)
dataChanges.add_command(d_commands.dataChanges_activities)


@cli.group("fileContainers")
def fileContainers():
    """
    File container (/fileContainers) operations to create, load, and list.
    """


fileContainers.add_command(f_commands.fileContainers_create)
fileContainers.add_command(f_commands.filecontainers_list)
fileContainers.add_command(f_commands.filecontainers_load)


@cli.group("wql")
def wql():
    """
    Operations to list (/dataSources) and run WQL queries (/data).
    """


wql.add_command(w_commands.dataSources)
wql.add_command(w_commands.data)


@cli.group("raas")
def raas():
    """
    Run custom or Workday delivered report.
    """


raas.add_command(r_commands.run)


if __name__ == "__main__":
    cli()
