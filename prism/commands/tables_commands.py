import json
import logging
import sys
import click

from prism import *

logger = logging.getLogger('prismCLI')


@click.command('get')
@click.option('-n', '--isName', is_flag=True, default=False,
              help='Flag to treat the table argument as a name.')
@click.option('-l', '--limit', type=int, default=None,
              help='The maximum number of object data entries included in the response, default=all.')
@click.option('-o', '--offset', type=int, default=None,
              help='The offset to the first object in a collection to include in the response.')
@click.option('-t', '--type', 'type_', default='summary',
              type=click.Choice(['summary', 'full', 'permissions'], case_sensitive=False),
              help='How much information returned for each table.')
@click.option('-c', '--compact', is_flag=True, default=False,
              help='Compact the table schema for use in edit (put) operations.')
@click.option('-s', '--search', is_flag=True,
              help='Enable substring search of NAME in api name or display name.')
@click.argument('table', required=False)
@click.pass_context
def tables_get(ctx, isname, table, limit, offset, type_, compact, search):
    """List the tables or datasets permitted by the security profile of the current user.

    [TABLE] Prism table ID or name (--isName flag) to list.
    """

    p = ctx.obj['p']

    # Query the tenant...see if the caller said to treat the
    # table as a name, AND that a table was provided.
    if not isname and table is not None:
        # When using an ID, the GET:/tables operation returns a simple
        # dictionary of the table definition.
        table = p.tables_get(id=table, type_=type_)

        if table is None:
            logger.error(f"Table ID {table} not found.")
            sys.exit(1)

        if compact:
            table = schema_compact(table)

        logger.info(json.dumps(table, indent=2))
    else:
        # When querying by name, the get operation returns a
        # dict with a count of found tables and a list of
        # tables.
        tables = p.tables_get(name=table, limit=limit, offset=offset, type_=type_, search=search)

        if tables['total'] == 0:
            logger.error(f"Table ID {table} not found.")
            return

        if compact:
            for tab in tables['data']:
                tab = schema_compact(tab)

        logger.info(json.dumps(tables, indent=2))


@click.command('create')
@click.option('-n', '--name',
              help='Table name - overrides name from schema.')
@click.option('-d', '--displayName',
              help='Specify a display name - defaults to name.')
@click.option('-t', '--tags', multiple=True,
              help='Tags to organize the table in the Data Catalog.')
@click.option('-e', '--enableForAnalysis', type=bool, is_flag=True, default=None,
              help='Enable this table for analytics.')
@click.option('-s', '--sourceName',
              help='The API name of an existing table to copy.')
@click.option('-w', '--sourceWID',
              help='The WID of an existing table to copy.')
@click.argument('file', required=False, type=click.Path(exists=True))
@click.pass_context
def tables_create(ctx, name, displayname, tags, enableforanalysis, sourcename, sourcewid, file):
    """
    Create a new table with the specified name.

    [FILE] Optional file containing a Prism schema definition for the new table.

    Note: A schema file, --sourceName, or --sourceWID must be specified.
    """
    p = ctx.obj['p']

    # We can assume a schema was found/built - get_schema sys.exits if there is a problem.
    schema = resolve_schema(p, file, sourcename, sourcewid)

    # Initialize a new schema with the particulars for this table operation.
    if name is not None:
        # If we got a name, set it in the table schema
        schema['name'] = name.replace(' ', '_')  # Minor clean-up
        logger.debug(f'setting table name to {schema["name"]}')
    elif 'name' not in schema:
        # The schema doesn't have a name and none was given - exit.
        # Note: this could be true if we have a schema of only fields.
        logger.error('Table --name must be specified.')
        sys.exit(1)

    if displayname is not None:
        # If we got a display name, set it in the schema
        schema['displayName'] = displayname
    elif 'displayName' not in schema:
        # Default the display name to the name if not in the schema.
        schema['displayName'] = name
        logger.debug(f'defaulting displayName to {schema["displayName"]}')

    if enableforanalysis is not None:
        schema['enableForAnalysis'] = enableforanalysis
    elif 'enableForAnalysis' not in schema:
        # Default to False - do not enable.
        schema['enableForAnalysis'] = False
        logger.debug('defaulting enableForAnalysis to False.')

    # Create the table.
    table_def = p.tables_post(schema)

    if table_def is not None:
        logger.info(json.dumps(table_def, indent=2))
    else:
        logger.error(f'Error creating table {name}.')
        sys.exit(1)


@click.command('edit')
@click.option('-t', '--truncate', is_flag=True, default=False,
              help='Truncate the table before updating.')
@click.argument('file', required=True, type=click.Path(exists=True, dir_okay=False, readable=True))
@click.pass_context
def tables_edit(ctx, file, truncate):
    """Edit the schema for an existing table.

    [FILE] File containing an updated schema definition for the table.
    """
    p = ctx.obj['p']

    # The user can specify a GET:/tables output file containing
    # the ID and other attributes that could be passed on the
    # command line.
    schema = resolve_schema(file=file)

    table = p.tables_put(schema, truncate=truncate)

    if table is None:
        logger.error(f'Error updating table.')
    else:
        logger.info(json.dumps(table, indent=2))


@click.command('patch')
@click.option('-n', '--isName',
              help='Flag to treat the table argument as a name.')
@click.option('--displayName', is_flag=False, flag_value="*-clear-*", default=None,
              help='Set the display name for an existing table.')
@click.option('--description', is_flag=False, flag_value="*-clear-*", default=None,
              help='Set the display name for an existing table.')
@click.option('--documentation', is_flag=False, flag_value="*-clear-*", default=None,
              help='Set the documentation for an existing table.')
@click.option('--enableForAnalysis', is_flag=False, default=None,
              type=click.Choice(['true', 'false'], case_sensitive=False))
@click.argument('table', required=True, type=str)
@click.argument('file', required=False, type=click.Path(exists=True, dir_okay=False, readable=True))
@click.pass_context
def tables_patch(ctx, isname, table, file,
                 displayname, description, documentation, enableforanalysis):
    """Edit the specified attributes of an existing table with the specified id (or name).

    If an attribute is not provided in the request, it will not be changed.  To set an
    attribute to blank (empty), include the attribute without specifying a value.

    TABLE  The ID or API name (use -n option) of the table to patch
    [FILE] Optional file containing patch values for the table.
    """

    p = ctx.obj['p']

    # Figure out the new schema either by file or other table.
    patch_data = {}

    # The user can specify a GET:/tables output file containing
    # the ID and other attributes that could be passed on the
    # command line.
    if file is not None:
        try:
            with open(file, "r") as patch_file:
                patch_data = json.load(patch_file)
        except Exception as e:
            logger.error(e)
            sys.exit(1)

        if not isinstance(patch_data, dict):
            logger.error('invalid patch file - should be a dictionary')
            sys.exit(1)

        valid_attributes = ['displayName', 'description', 'enableForAnalysis', 'documentation']

        for patch_attr in patch_data.keys():
            if patch_attr not in valid_attributes:
                logger.error(f'unexpected attribute {patch_attr} in patch file')
                sys.exit(1)

    def set_patch_value(attr, value):
        """Utility function to set or clear a table attribute."""
        if value == '*-clear-*':
            patch_data[attr] = ''
        else:
            patch_data[attr] = value

    # See if the user creating new patch variables or overriding
    # values from the patch file.

    # Note: specifying the option without a value creates a
    # patch value to clear the value in the table def.
    if displayname is not None:  # Specified on CLI
        set_patch_value('displayName', displayname)

    if description is not None:
        set_patch_value('description', description)

    if documentation is not None:
        set_patch_value('documentation', documentation)

    if enableforanalysis is not None:
        if enableforanalysis.lower() == 'true':
            patch_data['enableForAnalysis'] = 'true'
        else:
            patch_data['enableForAnalysis'] = 'false'

    # The caller must be asking for something to change!
    if len(patch_data) == 0:
        logger.error("Specify at least one schema value to update.")
        sys.exit(1)

    # Identify the existing table we are about to patch.
    if not isname:
        # No verification, simply assume the ID is valid.
        resolved_id = table
    else:
        # Before doing anything, table name must exist.
        tables = p.tables_get(name=table, limit=1, search=False)  # Exact match

        if tables['total'] == 0:
            logger.error(f'Table name "{table}" not found.')
            sys.exit(1)

        resolved_id = tables['data'][0]['id']

    table = p.tables_patch(id=resolved_id, patch=patch_data)

    if table is None:
        logger.error(f'Error updating table ID {resolved_id}')
    else:
        logger.info(json.dumps(table, indent=2))


@click.command('upload')
@click.option('-n', '--isName', is_flag=True, default=False,
              help='Flag to treat the table argument as a name.')
@click.option('-o', '--operation', default='TruncateAndInsert',
              help='Operation for the table operation - default to TruncateAndInsert.')
@click.argument('table', required=True)
@click.argument('file', nargs=-1, type=click.Path(exists=True))
@click.pass_context
def tables_upload(ctx, table, isname, operation, file):
    """
    Upload a file into the table using a bucket.

    [TABLE] A Prism Table identifier.
    [FILE] One or more CSV or GZIP.CSV files.
    """

    p = ctx.obj['p']

    # Convert the file(s) provided to a list of compressed files.

    if len(file) == 0:
        logger.error('No files to upload.')
        sys.exit(1)

    if isname:
        results = table_upload_file(p, table_name=table, operation=operation)
    else:
        results = table_upload_file(p, table_id=table, operation=operation)

    logger.debug(json.dumps(results, indent=2))


@click.command('truncate')
@click.option('-n', '--isName', is_flag=True, default=False,
              help='Flag to treat the table argument as a name.')
@click.argument('table', required=True)
@click.pass_context
def tables_truncate(ctx, table, isname):
    """
    Truncate the named table.

    [TABLE] The Prism Table ID or API name of the table to truncate.
    """
    p = ctx.obj['p']
    msg = f'Unable to truncate table "{table}" - see log for details.'

    # To do a truncate, we still need a bucket with a truncate operation.
    if isname:
        bucket = p.buckets_create(target_name=table, operation='TruncateAndInsert')
    else:
        bucket = p.buckets_create(target_id=table, operation='TruncateAndInsert')

    if bucket is None:
        logger.error(msg)
        sys.exit(1)

    bucket_id = bucket['id']

    # Don't specify a file to put a zero sized file into the bucket.
    p.buckets_files(bucket_id)

    # Ask Prism to run the delete statement by completing the bucket.
    bucket = p.buckets_complete(bucket_id)

    if bucket is None:
        logger.error(msg)
        sys.exit(1)


