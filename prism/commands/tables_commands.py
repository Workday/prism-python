import json
import logging
import sys
import os
import csv
import click
import pandas as pd

from . import util as u

logger = logging.getLogger('prismCLI')


@click.command('list')
@click.option('-w', '--wid',
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
@click.argument('name', required=False)
@click.pass_context
def tables_list(ctx, name, wid, limit, offset, type_, format_, search):
    """List the tables or datasets permitted by the security profile of the current user.

    [NAME] Prism table name to list.
    """

    if type_ in ('summary', 'permissions') and format_ in ('schema', 'csv'):
        # Summary results cannot generate schema or CSV output since there will be no fields.
        logger.error(f'Invalid combination of type "{type_}" and format "{format_}".')
        sys.exit(1)

    p = ctx.obj['p']

    # Query the tenant...
    tables = p.tables_list(name, wid, limit, offset, type_, search)

    # The return always has a total tables returned value.
    # note: tables_list never fails, it simply returns 0 tables if there is a problem.
    if tables['total'] == 0:
        return

    # Handle output
    if format_ == 'json':
        # The results could be one table or an array of multiple
        # tables - simply dump the returned object.

        click.echo(json.dumps(tables, indent=2))
    elif format_ == 'summary':
        for table in tables['data']:
            display_name = table['displayName']
            rows = table['stats']['rows'] if 'stats' in table and 'rows' in table['stats'] else 'Null'
            size = table['stats']['size'] if 'stats' in table and 'size' in table['stats'] else 'Null'
            refreshed = table['dateRefreshed'] if 'dateRefreshed' in table else 'unknown'
            enabled = table['enableForAnalysis'] if 'enableForAnalysis' in table else 'Null'

            click.echo(f'{display_name}, Enabled: {enabled}, Rows: {rows}, Size: {size}, Refreshed: {refreshed}')
    elif format_ == 'csv':
        df = pd.json_normalize(tables['data'])
        click.echo(df.to_csv(index=False))
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
                    # Shorten the Prism type syntax to remove the GUID id value.
                    fld['type']['id'] = f"Schema_Field_Type={fld['type']['descriptor']}"
                    del fld['type']['descriptor']

        click.echo(json.dumps(fields, indent=2))


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
    schema = get_schema(p, file, sourcename, sourcewid)

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
    table_def = p.tables_create(schema)

    if table_def is not None:
        click.echo(f'Table {name} created.')
    else:
        logger.error(f'Error creating table {name}.')


@click.command('update')
@click.option('-s', '--sourceName', help='The API name of an existing table to copy.')
@click.option('-w', '--sourceWID', help='The ID of an existing table to copy.')
@click.option('-t', '--truncate', is_flag=True, default=False, help='Truncate the table before updating.')
@click.argument('name', required=True)
@click.argument('file', required=False, type=click.Path(exists=True))
@click.pass_context
def tables_update(ctx, name, file, sourcename, sourcewid, truncate):
    """Edit the schema for an existing table.

    NAME   The API name of the table to update\b
    [FILE] Optional file containing an updated schema definition for the table.

    Note: A schema file, --sourceName, or --sourceWID must be specified.
    """

    p = ctx.obj['p']

    # Before doing anything, table name must exist.
    tables = p.tables_list(name=name)

    if tables['total'] == 0:
        logger.error(f'Table \"{name}\" not found.')
        sys.exit(1)

    table_id = tables['data'][0]['id']

    # Figure out the new schema either by file or other table.
    fields = get_schema(p, file, sourcename, sourcewid)

    p.tables_update(wid=table_id, schema=fields, truncate=truncate)

    click.echo(f'Table {name} updated.')


@click.command('upload')
@click.option('-o', '--operation', default='TruncateAndInsert',
              help='Operation for the table operation - default to TruncateAndInsert.')
@click.argument('name', required=True)
@click.argument('file', nargs=-1, type=click.Path(exists=True))
@click.pass_context
def tables_upload(ctx, name, operation, file):
    """
    Upload a file into the table using a bucket.

    NOTE: This operation creates ".csv.gz" files for each .csv file.
    """
    p = ctx.obj['p']

    # Convert the file(s) provided to a list of compressed files.
    target_files = u.get_files(file)

    if len(target_files) == 0:
        logger.error('No files to upload.')
        sys.exit(1)

    bucket = p.buckets_create(target_name=name, operation=operation)

    if bucket is None:
        logger.error('Bucket creation failed.')
        sys.exit(1)

    results = p.buckets_upload(bucket['id'], target_files)

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


def resolve_schema(p, file, source_name, source_wid):
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
        if source_name is None and source_wid is None:
            logger.error('No schema file provided and a table (--sourceName or --sourceWID) not specified.')
            sys.exit(1)

        if source_wid is not None:
            tables = p.tables_list(wid=source_wid, type_='full')  # Exact match on WID - and get the fields (full)
        else:
            tables = p.tables_list(name=source_name, type_='full')  # Exact match on API Name

        if tables['total'] == 0:
            logger.error('Invalid --sourceName or --sourceWID : table not found.')
            sys.exit(1)
        else:
            schema = tables['data'][0]

    return schema
