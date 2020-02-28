import click
import json
import prism

@click.group()
@click.option('--client_id', envvar="prism_client_id", type=str)
@click.option('--client_secret', envvar="prism_client_secret", type=str)
@click.option('--refresh_token', envvar="prism_refresh_token", type=str)
@click.pass_context
def main(ctx, client_id, client_secret, refresh_token):
    """CLI for interacting with Workday’s Prism API"""

    # initialize the prism class with your credentials
    p = prism.Prism(
        "https://wd5-impl-services1.workday.com",
        "workday",
        client_id,
        client_secret,
        refresh_token
    )

    # create the bearer token
    p.create_bearer_token()

    # store the prism object in the context
    ctx.obj = {'p': p}


@main.command()
@click.pass_context
def list(ctx):
    """List all datasets of type API"""

    # get the prism object
    p = ctx.obj['p']

    # list the datasets
    status = p.list_dataset()

    # print message
    click.echo(f"There are {status['total']} API datasets")
    click.echo(json.dumps(status['data'], indent=2, sort_keys=True))

@main.command()
@click.argument('dataset_name', type=str)
@click.argument('schema_path', type=click.Path())
@click.argument('data_path', type=click.Path())
@click.pass_context
def upload(ctx, dataset_name, schema_path, data_path):
    """Upload a gzip CSV file"""

    # get the prism object
    p = ctx.obj['p']

    # clean up the dataset name
    dataset_name = dateset_name.replace(" ", "_")

    # create an empty API dataset
    dataset = p.create_dataset(dataset_name)

    # read in your dataset schema
    schema = prism.load_schema(schema_path)

    # create a new bucket to hold your file
    bucket = p.create_bucket(schema, dataset["id"])

    # add your file the bucket you just created
    p.upload_file_to_bucket(bucket["id"], data_path)

    # complete the bucket and upload your file
    p.complete_bucket(bucket["id"])

    # check the status of the dataset you just created
    status = p.list_dataset(dataset["id"])

    # print message
    click.echo(f"{dataset_name} has successfully uploaded")
    click.echo(json.dumps(status['data'], indent=2, sort_keys=True))

if __name__ == "__main__":
    main()
