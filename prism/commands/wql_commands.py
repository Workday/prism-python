import click
import json
import sys
import logging
import re
import pandas as pd

logger = logging.getLogger('prismCLI')


@click.command("dataSources")
@click.option("-a", "--alias", default=None, type=str,
              help="The alias of the data source.")
@click.option("-s", "--searchString", default=None, type=str,
              help="The string to be searched in case insensitive manner within the descriptors of the data sources.")
@click.option("-l", "--limit", default=None, type=int,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None, type=int,
              help="The offset to the first object in a collection to include in the response.")
@click.option('-f', '--format', 'format_', default='json',
              type=click.Choice(['json', 'tabular'], case_sensitive=True),
              help='Gets the fields you have access to in the given data source.')
@click.argument("dataSource", required=False)
@click.pass_context
def dataSources(ctx, alias, searchstring, limit, offset, format_, datasource):
    """Returns a collection of data sources (/dataSources) for use in a WQL query.

    [DATASOURCE] The Workday ID of the resource.
    """
    p = ctx.obj["p"]

    if datasource is not None:
        data_sources = p.wql_dataSources(id=datasource, limit=limit, offset=offset)
    else:
        data_sources = p.wql_dataSources(alias=alias, searchString=searchstring, limit=limit, offset=offset)

    if format_ == 'json':
        logger.info(json.dumps(data_sources, indent=2))
    else:
        df = pd.json_normalize(data_sources['data'])
        logger.info(df.to_csv(index=False))


@click.command("fields")
@click.option("-d", "--sourceSearch", is_flag=True, default=False,
              help="The alias of the data source.")
@click.option("-a", "--alias", default=None, type=str,
              help="The alias of the data source.")
@click.option("-s", "--searchString", default=None, type=str,
              help="The string to be searched in case insensitive manner within the descriptors of the data sources.")
@click.option("-l", "--limit", default=None, type=int,
              help="The maximum number of object data entries included in the response.")
@click.option("-o", "--offset", default=None, type=int,
              help="The offset to the first object in a collection to include in the response.")
@click.option('-f', '--format', 'format_', default='json',
              type=click.Choice(['json', 'tabular'], case_sensitive=True),
              help='Gets the fields you have access to in the given data source.')
@click.argument("dataSource", required=True)
@click.pass_context
def dataSources_fields(ctx, sourcesearch, alias, searchstring, limit, offset, format_, datasource):
    """Returns a collection of data sources (/dataSources) for use in a WQL query.

    [DATASOURCE] The Workday ID of the resource.
    """
    p = ctx.obj["p"]

    if sourcesearch:
        data_sources = p.wql_dataSources(alias=datasource, searchString=datasource, limit=1, offset=0)

        if data_sources['total'] != 1:
            logger.error(f'Unexpected number of data sources: {data_sources["total"]}')
            sys.exit(1)

        ds_id = data_sources['data'][0]['id']
    else:
        ds_id = datasource

    fields = p.wql_dataSources_fields(id=ds_id, alias=alias, searchString=searchstring, limit=limit, offset=offset)

    if fields['total'] == 0:
        logger.error('No WQL fields found.')
        sys.exit(1)

    if format_ == 'json':
        logger.info(json.dumps(fields, indent=2))
    else:
        df = pd.json_normalize(fields['data'])
        logger.info(df.to_csv(index=False))


@click.command("data")
@click.option("-l", "--limit", default=None,
              help="The maximum number of object data entries included in the response, default=-1 (all).")
@click.option("-o", "--offset", default=None,
              help="The offset to the first object in a collection to include in the response.")
@click.option("-w", "--wql-file", "wql_file", default=None, type=click.Path(exists=True),
              help="Filename containing a WQL query.")
@click.option('-f', '--format', 'format_', default='json',
              type=click.Choice(['json', 'tabular'], case_sensitive=True),
              help='Gets the fields you have access to in the given data source.')
@click.argument("query", required=False)
@click.pass_context
def data(ctx, limit, offset, wql_file, format_, query):
    """
    Returns the data from a WQL query.

    [QUERY] WQL query string to execute (/data).

    Note: specify "select *" to automatically expand the column list.
    """
    p = ctx.obj["p"]

    if wql_file is None and query is None:
        click.echo("No query provided.")
        sys.exit(1)

    if query is not None:
        # Passed as an explicit string.
        query_resolved = query
    else:
        # Passed as a file name.
        with open(wql_file) as file:
            query_resolved = file.read().replace('\n', ' ')

    query_resolved = query_resolved.strip()

    # If the WQL statements starts with exactly "select *", attempt
    # to replace the asterisk with the field list.

    if query_resolved.lower().startswith('select *'):
        # Locate the "FROM {ds}" clause to get the data source name.

        # To query data from a data source:
        #   FROM dataSourceAlias
        # To query data from a data source with a data source filter:
        #   FROM dataSourceAlias(dataSourceFilter=filterAlias, filterPrompt1=value1, filterPrompt2=value2)
        # To query data from a data source using entry and effective date filters:
        #   FROM dataSourceAlias(effectiveAsOfDate=date, entryMoment=dateTime)

        from_regex = re.compile(r'\s+from[\s*|(](\w+)', flags=re.IGNORECASE)
        from_clause = from_regex.search(query_resolved)

        ds_alias = query_resolved[from_clause.start(1):from_clause.end(1)]
        logger.debug(f'Detected data source: {ds_alias}.')

        ds = p.wql_dataSources(alias=ds_alias, limit=1)

        if ds['total'] != 1:
            logger.error(f'Data source {ds_alias} not found.')
            sys.exit(1)

        ds_id = ds['data'][0]['id']

        fields = p.wql_dataSources_fields(id=ds_id)  # No limit gets all fields.

        if fields['total'] == 0:
            logger.error(f'No fields found for {ds_alias}.')
            sys.exit(1)

        columns = ''
        comma = ''

        for field in fields['data']:
            columns += comma + field['alias']
            comma = ','

        query_resolved = query_resolved.replace('*', columns, 1)

    rows = p.wql_data(query_resolved, limit, offset)

    if rows["total"] != 0:
        if format_ == 'tabular':
            df = pd.json_normalize(rows["data"])
            click.echo(df.to_csv(index=False))
        else:
            click.echo(json.dumps(rows, indent=2))
