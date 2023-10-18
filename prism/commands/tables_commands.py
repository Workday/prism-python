import json
import logging
import sys
import os
import csv
import click
import pandas as pd

logger = logging.getLogger('prismCLI')


@click.command('get')
@click.option('-n', '--name',
              help='Specific WID of Prism table or dataset to list.')
@click.option('-l', '--limit', type=int, default=None,
              help='The maximum number of object data entries included in the response, default=all.')
@click.option('-o', '--offset', type=int, default=None,
              help='The offset to the first object in a collection to include in the response.')
@click.option('-t', '--type', 'type_', default='summary',
              type=click.Choice(['summary', 'full', 'permissions'], case_sensitive=False),
              help='How much information returned for each table.')
@click.option('-f', '--format', 'format_', default='json',
              type=click.Choice(['json', 'summary', 'schema', 'csv'], case_sensitive=False),
              help='Format output as JSON, summary, schema, or CSV.')
@click.option('-s', '--search', is_flag=True,
              help='Enable substring search of NAME in api name or display name, default=False (exact match).')
@click.argument('id', required=False)
@click.pass_context
def tables_get(ctx, name, id, limit, offset, type_, format_, search):
    """List the tables or datasets permitted by the security profile of the current user.

    [NAME] Prism table name to list.
    """

    if type_ in ('summary', 'permissions') and format_ in ('schema', 'csv'):
        # Summary results cannot generate schema or CSV output since there will be no fields.
        logger.error(f'Invalid combination of type "{type_}" and format "{format_}".')
        sys.exit(1)

    p = ctx.obj['p']

    # Query the tenant...
    tables = p.tables_get(name, id, limit, offset, type_, search)

    if id is not None:
        if tables is None:
            logger.error(f"Table ID {id} not found.")
            sys.exit(1)
        else:
            # When using ID, the returned object is NOT an
            # array of tables - dump the single table object.
            logger.info(json.dumps(tables, indent=2))

        return

    # For any other type of GET:/tables, the return ALWAYS has a total
    # tables returned value.
    if tables['total'] == 0:
        return

    # Handle output
    if format_ == 'json':
        # The results could be one table or an array of multiple
        # tables - simply dump the returned object.

        logger.info(json.dumps(tables, indent=2))
    elif format_ == 'summary':
        for table in tables['data']:
            display_name = table['displayName']
            rows = table['stats']['rows'] if 'stats' in table and 'rows' in table['stats'] else 'Null'
            size = table['stats']['size'] if 'stats' in table and 'size' in table['stats'] else 'Null'
            refreshed = table['dateRefreshed'] if 'dateRefreshed' in table else 'unknown'
            enabled = table['enableForAnalysis'] if 'enableForAnalysis' in table else 'Null'

            logger.info(f'{display_name}, Enabled: {enabled}, Rows: {rows}, Size: {size}, Refreshed: {refreshed}')
    elif format_ == 'csv':
        df = pd.json_normalize(tables['data'])
        logger.info(df.to_csv(index=False))
    elif format_ == 'schema':
        # Dump out the fields of the first table in the result in
        # a format compatible with a schema used to created or edit
        # a table.

        table = tables['data'][0]  # Only output the first table.

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
                    # Convert the descriptor to shorten the Prism type syntax.
                    fld['type']['id'] = f"Schema_Field_Type={fld['type']['descriptor']}"
                    del fld['type']['descriptor']

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

    # We can assume a valid schema - get_schema sys.exits if there is a problem.
    schema = resolve_schema(p, file, sourcename, sourcewid)

    # Initialize a new schema with the particulars for this table operation.
    if name is not None:
        # If we got a name, set it in the table schema
        schema['name'] = name.replace(' ', '_')  # Minor clean-up
    elif 'name' not in schema:
        # The schema doesn't have a name and none was given - exit.
        logger.error('Table --name must be specified.')
        sys.exit(1)

    if displayname is not None:
        # If we got a display name, set it in the schema
        schema['displayName'] = displayname
    elif 'displayName' not in schema:
        # Default the display name to the name if not in the schema.
        schema['displayName'] = name

    if enableforanalysis is not None:
        schema['enableForAnalysis'] = enableforanalysis
    elif 'enableForAnalysis' not in schema:
        # Default to False - do not enable.
        schema['enableForAnalysis'] = False

    # Create the table.
    table_def = p.tables_post(schema)

    if table_def is not None:
        logger.info(f'Table {name} created.')
    else:
        logger.error(f'Error creating table {name}.')


@click.command('edit')
@click.option('-n', '--name', default=None,
              help='Table name - overrides name from schema.')
@click.option('-i', '--id', default=None,
              help='Prism table ID.')
@click.option('-t', '--truncate', is_flag=True, default=False,
              help='Truncate the table before updating.')
@click.option('--displayName', is_flag=False, flag_value="*-clear-*", default=None,
              help='Set the display name for an existing table.')
@click.option('--description', is_flag=False, flag_value="*-clear-*", default=None,
              help='Set the display name for an existing table.')
@click.option('--documentation', is_flag=False, flag_value="*-clear-*", default=None,
              help='Set the documentation for an existing table.')
@click.option('--enableForAnalysis', is_flag=False, default=None, required=False,
              type=click.Choice(['true', 'false'], case_sensitive=False))
@click.option('-f', '--file', type=click.Path(exists=True, dir_okay=False, readable=True))
@click.argument('id', required=False, type=str)
@click.pass_context
def tables_edit(ctx, name, id, file, truncate,
                displayname, description, documentation, enableforanalysis):
    """Edit the schema for an existing table.

    NAME   The API name of the table to update\b
    [FILE] Optional file containing an updated schema definition for the table.

    Note: A schema file, --sourceName, or --sourceWID must be specified.
    """

    p = ctx.obj['p']

    # Figure out the new schema either by file or other table.
    schema = None
    resolved_id = None

    # The user can specify a GET:/tables output file containing
    # the ID and other attributes that could be passed on the
    # command line.
    if file is not None:
        schema = resolve_schema(p, file)

        # If we got a file name, do a quick sanity check.
        if 'id' not in schema or 'fields' not in schema:
            logger.error(f'Specify a valid table schema file.')
            sys.exit(1)

        resolved_id = schema['id']

    # See if the user is overriding the ID we may have from
    # a specified schema file.
    if id is not None:
        # No verification, simply assume the ID is valid.
        resolved_id = id
    elif name is not None:
        # Before doing anything, table name must exist.
        tables = p.tables_get(name=name)  # Exact match

        if tables['total'] == 0:
            logger.error(f'Table name "{name}" not found.')
            sys.exit(1)

        resolved_id = tables['data'][0]['id']

    if resolved_id is None:
        logger.error('Specify a schema file, ID or name to update.')
        sys.exit(1)

    # If the caller sent specified attributes, do a patch not put.
    patch_data = {}

    def set_patch_value(attr, value):
        """Utility function to set or clear a table attribute."""
        if value == '*-clear-*':
            patch_data[attr] = ''
        else:
            patch_data[attr] = value

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

    if len(patch_data) == 0 and file is None:
        logger.error("Specify values to update or a schema file with updates.")
        sys.exit(1)

    if len(patch_data) > 0:
        table = p.tables_patch(id=resolved_id, patch=patch_data)
    else:
        table = p.tables_put(id=resolved_id, schema=schema, truncate=truncate)

    if table is None:
        logger.error(f'Error updating table ID {resolved_id}')
    else:
        logger.debug(json.dumps(table, indent=2))
        logger.info(f'Table {resolved_id} updated.')


@click.command('upload')
@click.option('-o', '--operation', default='TruncateAndInsert',
              help='Operation for the table operation - default to TruncateAndInsert.')
@click.argument('name', required=True)
@click.argument('file', nargs=-1, type=click.Path(exists=True))
@click.pass_context
def tables_upload(ctx, name, operation, file):
    """
    Upload a file into the table using a bucket.
    """

    p = ctx.obj['p']

    # Convert the file(s) provided to a list of compressed files.

    if len(file) == 0:
        logger.error('No files to upload.')
        sys.exit(1)

    bucket = p.buckets_create(target_name=name, operation=operation)

    if bucket is None:
        logger.error('Bucket creation failed.')
        sys.exit(1)

    results = p.buckets_upload(bucket['id'], file)

    if len(results) > 0:
        p.buckets_complete(bucket['id'])


@click.command('truncate')
@click.argument('name', required=True)
@click.pass_context
def tables_truncate(ctx, name):
    """
    Truncate the named table.

    [NAME] The API name of the Prism table to truncate.
    """
    p = ctx.obj['p']
    msg = f'Unable to truncate table "{name}" - see log for details.'

    # To do a truncate, we still need a bucket with a truncate operation.
    bucket = p.buckets_create(target_name=name, operation='TruncateAndInsert')

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

    schema = {'fields': []}  # Start with an empy schema definition.

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        # Force all the columns names from the CSV to lowercase to make
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

            # The following two items may not be in the CSV, the columns are not required and may not be present.

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

            match fld_type:
                case 'date':
                    if 'parseformat' in row and isinstance(row['parseformat'], str) and len(row['parseformat']) > 0:
                        field['parseFormat'] = row['parseformat']
                    else:
                        field['parseFormat'] = 'yyyy-MM-dd'

                case 'numeric':
                    if 'precision' in row:
                        field['precision'] = row['precision']

                        if 'scale' in row:
                            field['scale'] = row['scale']

                case 'instance':
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

    format_str = '{name},"{displayName}",{ordinal},{type},"{businessObject}",{precision},{scale},"{parseFormat}",{required},{externalId}\n'

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


def resolve_schema(p, file, source_name=None, source_id=None):
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

            if type(schema) is list:
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
