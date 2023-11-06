![Python package](https://github.com/Workday/prism-python/workflows/Python%20package/badge.svg)

# Prism-Python

Python client library and command line interface (CLI) for interacting with
Workday’s Prism API V3.

Workday provides the Prism Analytics REST API web service to work with
Workday Prism Analytics tables, data change tasks, and datasets. You can develop
a software program that uses the different REST endpoints to 
programmatically create Prism Analytics tables and datasets and modify 
data in them.

The Python **client library** is a REST API wrapper managing the HTTP methods,
URL endpoints and the data required by specific Workday Prism Analytics API
REST operations.  Using this client library in Python projects simplifies interactions
with the Workday Prism Analytics REST API providing the rich functionality
of Workday Prism Analytics to your Python project.

The **CLI** is a powerful tool for interacting with a Workday Prism Analytics 
REST API client library, allowing you to quickly and easily perform Workday 
Prism Analytics tasks from any command line.

## Install

To automatically retrieve and install the latest version of this
package directly GitHub, use the following command:

```bash
$ pip install git+https://github.com/Workday/prism-python.git
```

It is also possible to install a specific tagged release with:

```bash
$ pip install git+https://github.com/Workday/prism-python.git@0.3.0
```

## Requirements

Workday Prism Analytics REST APIs use OAuth authentication and the Workday 
configurable security model to authorize Workday Prism Analytics operations
in end-user applications.  The Workday Prism REST APIs act on behalf of 
a Workday user using the client. The user's security profile affects the 
REST API access to Workday resources.

The Prism client library, and by extension the CLI, require API Client 
credentials setup in the target Workday tenant.  The API Client credentials 
authorize programmatic access to the Workday tenant and provides the identity
of the Workday user to enforce security for all operations.

1. [Register a Workday Prism Analytics API Client.](https://doc.workday.com/admin-guide/en-us/workday-studio/integration-design/common-components/the-prismanalytics-subassembly/tzr1533120600898.html)
2. [Create Refresh Token](https://doc.workday.com/reader/J1YvI9CYZUWl1U7_PSHyHA/L_RKkfJI6bKu1M2~_mfesQ)
3. [Obtain the Workday REST API Endpoint.](https://doc.workday.com/reader/J1YvI9CYZUWl1U7_PSHyHA/L_RKkfJI6bKu1M2~_mfesQ)


## Python Example

### Create a new table with Prism API Version 3

```{python}
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

# create a new table based on the schema.json file
table = prism.tables_create(
    p,
    table_name="my_new_table",
    file="/path/to/schema.json"
)

# print JSON response body describing the new table.
print(table)
```

### Manage data in an existing table with Prism API Version 3
Table Operations Available: `TruncateandInsert`, `Insert`, `Update`, `Upsert`,
`Delete`.

```{python}
prism.upload_file(
    p,
    file="/path/to/data.csv.gz", 
    table_id=table["id"],
    operation="Insert"
)
```

## CLI Example

```bash
# get help with the CLI
$ prism --help

# get help for the tables command
$ prism tables --help

# list Prism tables you have access to.
$ prism tables get

# create a new Prism table 
$ prism tables create my_new_table /path/to/schema.json

# upload data to the new table
$ prism tables upload 83dd72bd7b911000ca2d790e719a0000 /path/to/file1.csv.gz
```

## Bugs
Please report any bugs that you find [here](https://github.com/Workday/prism-python/issues).
Or, even better, fork the repository on [GitHub](https://github.com/Workday/prism-python)
and create a pull request (PR). We welcome all changes, big or small, and we
will help you make the PR if you are new to `git`.

## License
Released under the Apache-2.0 license (see [LICENSE](https://github.com/Workday/prism-python/blob/master/LICENSE))
