import click


@click.command("run", help="Run RaaS report as system or as a specific user.")
@click.option("-s", "--system", is_flag=True, default=False, help="Run delivered Workday report.")
@click.option("-u", "--user", default=None, help="Run custom report as named user.")
@click.option("-f", "--format", "format_", default=None, help="Output query results as CSV.")
@click.argument("report", nargs=1)
@click.argument('params', nargs=-1)
@click.pass_context
def run(ctx, system, user, format_, report, params):
    p = ctx.obj["p"]

    if system and user is not None:
        print("Please specify only system or user, not both.")
        return

    if not system and user is None:
        print("Please specify either system or user.")

    report_output = p.raas_run(report, system, user, params, format_)

    print(report_output)
