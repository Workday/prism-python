![Python package](https://github.com/Workday/prism-python/workflows/Python%20package/badge.svg)

# Prism-Python

Python client library  and command line interface (CLI) for interacting with Workday’s Prism API.

## Install
You may install the latest version by cloning this repository from GitHub
and using `pip` to install from the local directory:

```bash
git clone https://github.com/Workday/prism-python.git
cd prism-python
pip install .
```

It is also possible to install the latest version directly from GitHub with:

```bash
pip install git+git://github.com/Workday/prism-python.git
```

## Requirements

1. [Register a Workday Prism Analytics API Client.](https://doc.workday.com/reader/J1YvI9CYZUWl1U7_PSHyHA/qAugF2pRAGtECVLHKdMO_A)

In Workday, register an integrations API client with Prism Analytics as its
scope. Obtain the Client ID, Client Secret, and Refresh Token values that the
Prism class requires as parameters.

2. [Obtain the Workday REST API Endpoint.](https://doc.workday.com/reader/J1YvI9CYZUWl1U7_PSHyHA/L_RKkfJI6bKu1M2~_mfesQ)

In Workday, obtain the Workday REST API endpoint that the Prism class requires
as a parameter.

3. For ease of use, set the following environment variables using the values obtained above:

```bash
$ export workday_base_url=<INSERT WORKDAY BASE URL HERE>
$ export workday_tenant_name=<INSERT WORKDAY TENANT NAME HERE>
$ export prism_client_id=<INERT PRISM CLIENT ID HERE>
$ export prism_client_secret=<INSERT PRISM CLIENT SECRET HERE>
$ export prism_refresh_token=<INSERT PRISM REFRESH TOKEN HERE>
```

## Example

### Using the CLI

```bash
# view the help page
$ prism --help

# list all datasets of type API
$ prism list

# upload a gzip CSV file
$ prism upload my_new_dataset /path/to/schema.json /path/to/file.csv.gz
```

### Using as a Python Package

```python
import os
import prism

# initialize the prism class with your credentials
p = prism.Prism(
    os.getenv("workday_base_url"),
    os.getenv("workday_tenant_name"),
    os.getenv("prism_client_id"), 
    os.getenv("prism_client_secret"),
    os.getenv("prism_refresh_token") 
)

# create the bearer token
p.create_bearer_token()

# create an empty API dataset
dataset = p.create_dataset("my_new_dataset")

# read in your dataset schema
schema = prism.load_schema("/path/to/schema.json")

# create a new bucket to hold your file
bucket = p.create_bucket(schema, dataset["id"])

# add your file the bucket you just created
p.upload_file_to_bucket(bucket["id"], "/path/to/file.csv.gz")

# complete the bucket and upload your file
p.complete_bucket(bucket["id"])

# check the status of the dataset you just created
status = p.list_dataset(dataset["id"])
print(status)
```

## Bugs
Please report any bugs that you find [here](https://github.com/Workday/prism-python/issues).
Or, even better, fork the repository on [GitHub](https://github.com/Workday/prism-python)
and create a pull request (PR). We welcome all changes, big or small, and we
will help you make the PR if you are new to `git`.

## License
Released under the Apache-2.0 license (see [LICENSE](https://github.com/Workday/prism-python/blob/master/LICENSE))
