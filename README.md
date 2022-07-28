![Python package](https://github.com/Workday/prism-python/workflows/Python%20package/badge.svg)

# Prism-Python

Python client library and command line interface (CLI) for interacting with
Workday’s Prism API V2.

## Install
You may install the latest version directly from GitHub with:

```bash
pip install git+https://github.com/Workday/prism-python.git
```

It is also possible to install a specific tagged release with:

```bash
pip install git+https://github.com/Workday/prism-python.git@0.2.0
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
export workday_base_url=<INSERT WORKDAY BASE URL HERE>
export workday_tenant_name=<INSERT WORKDAY TENANT NAME HERE>
export prism_client_id=<INERT PRISM CLIENT ID HERE>
export prism_client_secret=<INSERT PRISM CLIENT SECRET HERE>
export prism_refresh_token=<INSERT PRISM REFRESH TOKEN HERE>
```

## Python Example

### Create a new table with Prism API Version 2

```python
import os
import prism

# initialize the prism class with your credentials
p = prism.Prism(
    os.getenv("workday_base_url"),
    os.getenv("workday_tenant_name"),
    os.getenv("prism_client_id"),
    os.getenv("prism_client_secret"),
    os.getenv("prism_refresh_token"),
    version="v2"
)

# read in your table schema
schema = prism.load_schema("/path/to/schema.json")

# create an empty API table with your schema
table = prism.create_table("my_new_table", schema=schema["fields"])

# print details about new table
print(table)
```

### Manage data in an existing table with Prism API Version 2
Table Operations Available: `TruncateandInsert`, `Insert`, `Update`, `Upsert`,
`Delete`.

To use the `Update`, `Upsert`, or `Delete` operations, you must specify an
external id field within your table schema.

```python
# upload GZIP CSV file to your table
prism.upload_file(p, "/path/to/file.csv.gz", table["id"], operation="TruncateandInsert")
```

## CLI Example

The command line interface (CLI) provides another way to interact with the Prism API.
The CLI expects your credentials to be stored as environment variables, but they can
also be passed into the CLI manually through the use of optional arguments.

```bash
# get help with the CLI
prism --help

# list the Prism API tables that you have access to
prism list

# create a new Prism API table
prism create my_new_table /path/to/schema.json

# upload data to a Prism API table
prism upload /path/to/file.csv.gz bbab30e3018b01a723524ce18010811b
```

## Bugs
Please report any bugs that you find [here](https://github.com/Workday/prism-python/issues).
Or, even better, fork the repository on [GitHub](https://github.com/Workday/prism-python)
and create a pull request (PR). We welcome all changes, big or small, and we
will help you make the PR if you are new to `git`.

## License
Released under the Apache-2.0 license (see [LICENSE](https://github.com/Workday/prism-python/blob/master/LICENSE))
