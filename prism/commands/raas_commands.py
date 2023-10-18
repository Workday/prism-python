import click
import logging

logger = logging.getLogger('prismCLI')


@click.command("run", help="Run a system or custom RaaS report.")
@click.option("-u", "--user", default=None, help="Run custom report as named user - default to delivered reports.")
@click.option("-f", "--format", "format_", default=None, help="Output query results as CSV.")
@click.argument("report", nargs=1, required=True)
@click.argument('params', nargs=-1, required=False)
@click.pass_context
def run(ctx, user, format_, report, params):
    """
    Run a Workday report.

    [REPORT] Report name to run.
    [PARAMS] Parameters expected by the report as list.
    """
    p = ctx.obj["p"]

    # Return to a variable for easy debugging.
    report_output = p.raas_run(report, user, params, format_)

    # Don't log the output - pusht
    click.echo(report_output)
