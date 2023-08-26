import click


@click.command("run", help="View the buckets permitted by the security profile of the current user.")
@click.option("-u", "--user", default=None, help="Output query results as CSV.")
@click.option("-f", "--format", "format_", default=None, help="Output query results as CSV.")
@click.argument("report", required=True)
@click.pass_context
def run(ctx, user, format_, report):
    p = ctx.obj["p"]

    report_output = p.raas_run(report, user, format_)

    print(report_output)
