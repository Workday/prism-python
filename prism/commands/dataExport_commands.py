import click
import json
import logging

logger = logging.getLogger('prismCLI')

@click.command('get')
@click.option('-l', '--limit', type=int, default=None,
              help='The maximum number of object data entries included in the response, default=all.')
@click.option('-o', '--offset', type=int, default=None,
              help='The offset to the first object in a collection to include in the response.')
@click.option('-t', '--type', 'type_', default='summary',
              type=click.Choice(['summary', 'full'], case_sensitive=False),
              help='How much information returned for each table.')
@click.option('-f', '--format', 'format_', default='json',
              type=click.Choice(['json', 'summary', 'schema', 'csv'], case_sensitive=False),
              help='Format output as JSON, summary, schema, or CSV.')
@click.pass_context
def dataExport_get(ctx, limit, offset, type_, format_):
    """List the tables or datasets permitted by the security profile of the current user.

    [NAME] Prism table name to list.
    """

    p = ctx.obj['p']

    data_export_list = p.dataExport_get(limit=limit, offset=offset, type_=type_)

    logger.info(json.dumps(data_export_list, indent=2))


@click.command('create')
@click.pass_context
def dataExport_create(ctx):
    logger.info("here")