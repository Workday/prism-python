import click


@click.group()
def main():
    """CLI for interacting with Workday’s Prism API"""
    pass


@main.command()
def list():
    """List all datasets of type API"""
    click.echo("These are your datasets...")


@main.command()
def upload():
    """Upload a gzip CSV file"""
    click.echo('Uploading your file...')


if __name__ == "__main__":
    main()
