![Python package](https://github.com/Workday/prism-python/workflows/Python%20package/badge.svg)

# Prism-Python

Python client library and command line interface (CLI) for interacting with
Workday’s Prism API V3.

Workday provides the Prism Analytics REST API web service that works with
Prism Analytics tables, data change tasks, and datasets. You can develop
a software program that uses the different resource endpoints to 
programmatically create Prism Analytics tables and datasets and modify 
data in them.

The Python client library and CLI work together to provide no-code/low-code 
access to the Workday Prism Analytics REST API.

The Python client library is a REST API wrapper managing the HTTP methods,
URL endpoints and the data required by specific Workday Prism Analytics API
REST operations.  Using this library in Python projects simplifies interactions
with the API while providing the rich functionality of the Workday Prism Analytics
REST API.

The CLI is a powerful tool for interacting with a Workday Prism Analytics REST API
client library, allowing you to quickly and easily perform Workday Prism Analytics
tasks from the command line.

## Workday Prism Analytics REST API Requirements

The Prism client library requires an api

1. [Register a Workday Prism Analytics API Client.](https://doc.workday.com/admin-guide/en-us/workday-studio/integration-design/common-components/the-prismanalytics-subassembly/tzr1533120600898.html)

In Workday, register an integrations API client with Prism Analytics as its
scope. Obtain the Client ID, Client Secret, and Refresh Token values that the
Prism client library requires as parameters.

2. [Obtain the Workday REST API Endpoint.](https://doc.workday.com/reader/J1YvI9CYZUWl1U7_PSHyHA/L_RKkfJI6bKu1M2~_mfesQ)

In Workday, obtain the Workday REST API endpoint that the Prism class requires
as a parameter.

Before configuring the CLI or using the Prism client library, ensure you have the following values:

- Base URL
- Tenant Name
- Client ID
- Client Secret
- Refresh Token

## Python Prerequisites ##

First determine whether you have up-to-date versions of Python, pip, and Git.
If you need to install Python and Git, please refer to the official
installation guides.

### Python ###
Python comes preinstalled on most Linux distributions, and is available as a package on all others. 
You can check which version of Python (if any) is installed, 
by entering the following command in a terminal or command window:

```bash
[user@host ~]$ python --version 
Python 3.11.1
```

or

```bash
[user@host ~]$ python3 --version 
Python 3.9.16
```

### Pip ##

**pip** is the preferred installer program. Starting with Python 3.4, it is included by default with the Python binary installers.
You can check if pip is already installed and up-to-date by entering the following command:

```bash
[user@host ~]$ pip --version
pip 23.3.1 from /<directory>/python3.11/site-packages/pip (python 3.11)
```

or

```bash
[user@host ~]$ pip3 --version
pip 23.3.1 from /<directory>/python3.9/site-packages/pip (python 3.9)
```

### Git Installation ###

Before installing Git, you should first determine whether you have it installed by running the following git command:

```bash
[user@host ~]$ git --version
git version 2.40.1
```

## Prism-Python Install ##

You may install the latest version directly from GitHub with:

```bash
pip install git+https://github.com/Workday/prism-python.git
```

It is also possible to install a specific tagged release with:

```bash
pip install git+https://github.com/Workday/prism-python.git@0.2.0
```

## Configuration ##

The CLI allows you to set provide user to change its behaviour via 3 mechanisms:

1. command line options
2. environment variables
3. configuration files 

### Command line options ###
always used regardless of other configurations

 ```
prism --base_url=<my base url> \
       --tenant_name <my tenant>...
```

### Environment variables ###

Used if present and not also on the command line

2. For ease of use, set the following environment variables using the values obtained above:

```bash
export workday_base_url=<INSERT WORKDAY BASE URL HERE>
export workday_tenant_name=<INSERT WORKDAY TENANT NAME HERE>
export prism_client_id=<INERT PRISM CLIENT ID HERE>
export prism_client_secret=<INSERT PRISM CLIENT SECRET HERE>
export prism_refresh_token=<INSERT PRISM REFRESH TOKEN HERE>
export prism_log_file=<INSERT PRISM REFRESH TOKEN HERE>
export prism_log_level=INFO|DEBUG|WARN|ERROR
```

### Configuration file ###

automatically looks for prism.ini - can be overridden with --config_file option.
NOTE the client secret and refresh tokens are the same as passwords and should be protected

    [default]
    workday_base_url = https://<workday base url>
    workday_tenant_name = <tenant name>
    prism_client_id = MTFmZWZjZTItZTk0NS00MWQ5LTkwMzItNTc5NWU4MGI3ZWYx
    prism_client_secret = c9fta3s2b5j5zfppthi19zdflncjljzgml4rk430mk9y1n5fm0lp9kstzzvmo0th0389mbve6gr5rg5kax9jmsn9l5om3vsanmq
    prism_refresh_token = we9o31mrs15z7g9qpcd6jedaf74mhv4weadki7uwldhbz99mn0s2u3skjy9zshst2r2wgda502q44g4m8pka2g26xvyzgboakc
    prism_log_level = DEBUG
    
    [integration]
    workday_base_url = https://<workday base url>
    workday_tenant_name = <integration tenant name>
    prism_client_id = NTFmMDYxZTktM2FjNi00MDJiLWI0YjctMGYwYTkyMmZlYmUy
    prism_client_secret = qym8c79g9inthk6ytodjmwhzhcss4qd8x06cepnvhd8g69hhp8ihle701sna8fv2myfyktj8br3fogz7yhzo5oo1oien3f4kkmi
    prism_refresh_token = jt8bkmo3q7ejqn0tcs3e171a1ytgzl18q942w44wbkfy0zflgyhkx82ldjllwlxnl91ngbp6x74ilfxca20smmom9mvzqfcm9s5
    prism_log_level = INFO

## Python client library example

### Create a new Prism table
The following Python script uses the Prism-Python client library to create
a new Workday Prism Analytics table and load the contents of a CSV file.

```python
import os
import prism

# STEP 1 - Initialize the Prism-Python client library
# using environment variables.
p = prism.Prism(
    os.getenv("workday_base_url"),
    os.getenv("workday_tenant_name"),
    os.getenv("prism_client_id"),
    os.getenv("prism_client_secret"),
    os.getenv("prism_refresh_token")
)

# STEP 2 - Create a new table using the definition of fields 
# provided by the schema.jsob file.
table = prism.tables_create(table_name="my_new_table", file="/path/to/schema.json")

# Print JSON result about the new table
print(table)

# STEP 3 - Use the convenience function prism.upload_file 
# to upload a local file to the table.  Notice the operation
# is Insert on the first load.
prism.upload_file(p, "/path/to/file.csv.gz", table["id"], operation="Insert")
```

### Manage data in an existing table
A
Table Operations Available: `TruncateandInsert`, `Insert`, `Update`, `Upsert`,
`Delete`.

To use the `Update`, `Upsert`, or `Delete` operations, you must specify an
external id field within your table schema.

```python

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

## Notes on schema files
1. Can be a full table definition including name, displayName and fields attributes
2. Can be a list of only field definitions
3. Field definitions are either full or compact(should i say this)

## Bugs
Please report any bugs that you find [here](https://github.com/Workday/prism-python/issues).
Or, even better, fork the repository on [GitHub](https://github.com/Workday/prism-python)
and create a pull request (PR). We welcome all changes, big or small, and we
will help you make the PR if you are new to `git`.

## License
Released under the Apache-2.0 license (see [LICENSE](https://github.com/Workday/prism-python/blob/master/LICENSE))
