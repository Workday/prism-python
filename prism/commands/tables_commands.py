import json
import logging
import sys
import os
import csv
import click
import pandas as pd

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
@click.option('-f', '--format', 'format_', default='json',
              type=click.Choice(['json', 'tabular', 'schema'], case_sensitive=False),
              help='Format output as JSON, tabular, or bucket schema.')
@click.option('-s', '--search', is_flag=True,
              help='Enable substring search of NAME in api name or display name, default=False (exact match).')
@click.argument('table', required=False)
@click.pass_context
def tables_get(ctx, isname, table, limit, offset, type_, format_, search):
    """List the tables or datasets permitted by the security profile of the current user.

    [NAME] Prism table name to list.
    """

    if type_ in ('summary', 'permissions') and format_ == 'schema':
        # Summary results cannot generate schema since there will be no fields.
        logger.error(f'Invalid combination of type "{type_}" and format "{format_}".')
        sys.exit(1)

    p = ctx.obj['p']

    # Query the tenant...see if the caller said to treat the
    # table as a name, AND that a table was provided.
    if not isname and table is not None:
        # When using an ID, the get operation returns a simple
        # dictionary of the table definition.
        table = p.tables_get(id=table, type_=type_)

        if table is None:
            logger.error(f"Table ID {table} not found.")
            sys.exit(1)

        if format_ == 'schema':
            logger.info(json.dumps(get_fields(table), indent=2))
        elif format_ == 'tabular':
            df = pd.json_normalize(table)
            logger.info(df.to_csv(index=False))
        else:
            logger.info(json.dumps(table, indent=2))
    else:
        # When querying by name, the get operation returns a
        # dict with a count of found tables and a list of
        # tables.
        tables = p.tables_get(name=table, limit=limit, offset=offset, search=search)

        if tables['total'] == 0:
            logger.error(f"Table ID {table} not found.")
            return

        if format_ == 'json':
            logger.info(json.dumps(tables, indent=2))
        elif format_ == 'tabular':
            df = pd.json_normalize(tables['data'])
            logger.info(df.to_csv(index=False))
        elif format_ == 'schema':
            fields = []

            for tab in tables['data']:
                fields.append(get_fields(tab))

            logger.info(json.dumps(fields, indent=2))


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


@click.command('put')
@click.option('-t', '--truncate', is_flag=True, default=False,
              help='Truncate the table before updating.')
@click.argument('file', required=True, type=click.Path(exists=True, dir_okay=False, readable=True))
@click.pass_context
def tables_put(ctx, file, truncate):
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
    """Edit the schema for an existing table.

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
        bucket = p.buckets_create(target_id=table, operation=operation)
    else:
        bucket = p.buckets_create(target_name=table, operation=operation)

    if bucket is None:
        logger.error('Bucket creation failed.')
        sys.exit(1)

    results = p.buckets_upload(bucket['id'], file)

    if len(results) > 0:
        p.buckets_complete(bucket['id'])


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
    p.buckets_upload(bucket_id)

    # Ask Prism to run the delete statement by completing the bucket.
    bucket = p.buckets_complete(bucket_id)

    if bucket is None:
        logger.error(msg)
        sys.exit(1)


def schema_from_csv(prism, file):
    """Convert a CSV list of fields into a proper Prism schema JSON object"""

    if not os.path.exists(file):
        logger.error(f'FIle {file} not found - skipping.')
        sys.exit(1)

    schema = {'fields': []}  # Start with an empty schema definition.

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # Force all the columns names (first row) from the CSV to lowercase to make
        # lookups consistent regardless of the actual case of the columns.
        reader.fieldnames = [f_name.lower() for f_name in reader.fieldnames]

        # The minimum definition is a name column - exit if not found.  No other
        # column definition is required to build a valid field list.
        if 'name' not in reader.fieldnames:
            logger.error(f'CSV file {file} does not contain a name column header in first line.')
            sys.exit(1)

        # Prism fields always have an ordinal sequence assigned to each field.
        ordinal = 1

        for row in reader:
            if len(row['name']) == 0:
                logger.error('Missing column name in CSV file.')
                sys.exit(1)

            # Start the new field definition with what we know so far.
            field = {
                'ordinal': ordinal,
                'name': row['name'],
                'displayName': row['displayname'] if 'displayname' in row else row['name']
            }

            # The following two columns are not required and may not be present.

            if 'required' in row and isinstance(row['required'], str) and row['required'].lower() == 'true':
                field['required'] = True
            else:
                field['required'] = False

            if 'externalid' in row and isinstance(row['externalid'], str) and row['externalid'].lower() == 'true':
                field['externalId'] = True
            else:
                field['externalId'] = False

            fld_type = 'none'

            prism_data_types = ['boolean', 'integer', 'text', 'date', 'long', 'decimal',
                                'numeric', 'instance', 'currency', 'multi_instance']

            if 'type' in row and row['type'].lower() in prism_data_types:
                field['type'] = {'id': f'Schema_Field_Type={row["type"]}'}
                fld_type = row['type'].lower()
            else:
                # Default all "un-typed" fields to text.
                field['type'] = {'id': 'Schema_Field_Type=Text'}

            if fld_type == 'date':
                if 'parseformat' in row and isinstance(row['parseformat'], str) and len(row['parseformat']) > 0:
                    field['parseFormat'] = row['parseformat']
                else:
                    field['parseFormat'] = 'yyyy-MM-dd'
            elif fld_type == 'numeric':
                if 'precision' in row:
                    field['precision'] = row['precision']

                    if 'scale' in row:
                        field['scale'] = row['scale']
            elif fld_type == 'instance':
                # We need all the data sources to resolve the business objects
                # to include their WID.
                data_sources = prism.datasources_list()

                if data_sources is None or data_sources['total'] == 0:
                    click.echo('Error calling WQL/dataSources')
                    return

                # Find the matching businessObject
                bo = [ds for ds in data_sources['data']
                      if ds['businessObject']['descriptor'] == row['businessObject']]

                if len(bo) == 1:
                    field['businessObject'] = bo[0]['businessObject']

            schema['fields'].append(field)
            ordinal += 1

    return schema


def csv_from_fields(fields):
    """Convert a Prism field list to CSV representation."""

    format_str = '{name},"{displayName}",{ordinal},{type},"{businessObject}",'
    format_str += '{precision},{scale},"{parseFormat}",{required},{externalId}\n'

    # Start with the CSV column headings.
    csv_str = 'name,displayName,ordinal,type,businessObject,precision,scale,parseFormat,required,externalId\n'

    for field in fields:
        # Suppress the Prism audit columns.
        if field['name'].startswith('WPA_'):
            continue

        field_def = {'name': field['name'],
                     'displayName': field['displayName'],
                     'ordinal': field['ordinal'],
                     'type': field['type']['descriptor'],
                     'businessObject': field['businessObject']['descriptor'] if 'businessObject' in field else '',
                     'precision': field['precision'] if 'precision' in field else '',
                     'scale': field['scale'] if 'scale' in field else '',
                     'parseFormat': field['parseFormat'] if 'parseFormat' in field else '',
                     'required': field['required'],
                     'externalId': field['externalId']
                     }

        # Add the new field to the CSV text.
        csv_str += format_str.format_map(field_def)

    return csv_str


def resolve_schema(p=None, file=None, source_name=None, source_id=None):
    """Get or extract a schema from a file or existing Prism table."""

    # Start with a blank schema definition.
    schema = {}

    # A file always takes precedence over sourceName and sourceWID
    # options, and must BE, or contain a valid schema.

    if file is not None:
        if file.lower().endswith('.json'):
            try:
                with open(file) as json_file:
                    schema = json.load(json_file)
            except Exception as e:
                logger.error(f'Invalid schema file: {e}.')
                sys.exit(1)

            # The JSON file could be a complete table definitions (GET:/tables - full) or just
            # the list of fields.  If we got a list, then we have a list of fields we
            # use to start the schema definition.

            if isinstance(schema, list):
                schema['fields'] = schema
            else:
                # This should be a full schema, perhaps from a table list command.
                if 'name' not in schema and 'fields' not in schema:
                    logger.error('Invalid schema - name and fields attribute not found.')
                    sys.exit(1)
        elif file.lower().endswith('.csv'):
            schema = schema_from_csv(p, file)
        else:
            logger.error('Invalid file extension - valid extensions are .json or .csv.')
            sys.exit(1)
    else:
        # No file was specified, check for a Prism source table.
        if source_name is None and source_id is None:
            logger.error('No schema file provided and a table (--sourceName or --sourceId) not specified.')
            sys.exit(1)

        if source_id is not None:
            schema = p.tables_list(id=source_id, type_='full')  # Exact match on WID - and get the fields (full)

            if schema is None:
                logger.error(f'Invalid --sourceId {source_id} : table not found.')
                sys.exit(1)
        else:
            tables = p.tables_list(name=source_name, type_='full')  # Exact match on API Name

            if tables['total'] == 0:
                logger.error(f'Invalid --sourceName {source_name} : table not found.')
                sys.exit(1)

            schema = tables['data'][0]

    return schema


def get_fields(table):
    if 'fields' not in table:
        logger.error('get_fields: table object does not contain fields attribute.')
        return None

    # Remove the Prism audit columns.
    fields = [fld for fld in table['fields'] if not fld['name'].startswith('WPA_')]

    # Remove tenant specific values - these are not needed
    # if the user wants to update a table definition.
    for fld in fields:
        if 'fieldId' in fld:
            del fld['fieldId']

        if 'id' in fld:
            del fld['id']

        if 'type' in fld:
            if 'descriptor' in fld['type']:
                # Convert the descriptor to the shortened Prism type syntax.
                fld['type']['id'] = f"Schema_Field_Type={fld['type']['descriptor']}"
                del fld['type']['descriptor']

    return fields
