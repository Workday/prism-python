![Python package](https://github.com/Workday/prism-python/workflows/Python%20package/badge.svg)

# Prism-Python

Python client library and command line interface (CLI) for interacting with
Workday’s Prism API V3.

Workday provides the Prism Analytics REST API web service to work with
Workday Prism Analytics tables, data change tasks, and datasets. You can develop
a software program that uses the different REST endpoints to 
programmatically create Prism Analytics tables and datasets and modify 
data in them.

The Python client library and CLI work together to provide no-code/low-code 
access to the Workday Prism Analytics REST API.

The Python **client library** is a REST API wrapper managing the HTTP methods,
URL endpoints and the data required by specific Workday Prism Analytics API
REST operations.  Using this client library in Python projects simplifies interactions
with the Workday Prism Analytics REST API providing the rich functionality
of Workday Prism Analytics to your Python project.

The **CLI** is a powerful tool for interacting with a Workday Prism Analytics 
REST API client library, allowing you to quickly and easily perform Workday 
Prism Analytics tasks from any command line.

## Workday Prism Analytics REST API Requirements

Workday Prism Analytics REST APIs use OAuth authentication and the Workday 
configurable security model to authorize Workday Prism Analytics operations
in end-user applications.  The Workday Prism REST APIs act on behalf of 
a Workday user using the client. The user's security profile affects the 
REST API access to Workday resources.

The Prism client library, and by extension the CLI, require API Client 
credentials setup in the target Workday tenant.  The API Client credentials 
authorize programmatic access to the Workday tenant and provides the identity
of the Workday user to enforce security for all operations.

### [Register a Workday Prism Analytics API Client.](https://doc.workday.com/admin-guide/en-us/workday-studio/integration-design/common-components/the-prismanalytics-subassembly/tzr1533120600898.html) ###

In the target Workday Prism enabled tenant, register an integrations API client
with Prism Analytics as its scope (task _Register API Client for Integrations_) to 
create the Client ID and Client Secret values allowing REST API access to the tenant.

![Register API](https://workday-prism-python.s3.amazonaws.com/Prism-Python-RegisterAPI.png)

After clicking the OK button, the confirmation screen shows the
two important REST API credentials: **Client ID** and **Client Secret**.

![Client ID](https://workday-prism-python.s3.amazonaws.com/Prism-Python-Secret.png)

**Record the secret value** for use with the Prism-Python client library.

> **Note**: Workday **never** shows the secret value again after clicking the Done button.

> **Note**: As a Workday best practice, try to minimize the number
> of unique API Clients since, for auditing reasons, they cannot be removed.

> **Note**: If the client secret is ever lost or compromised, a new secret
> can be generated.  However, a new secret invalidates any application
> using the old secret.

> **Note**: Protect the Client ID and Client Secret values the same way as
> any password.

### [Create Refresh Token](https://doc.workday.com/reader/J1YvI9CYZUWl1U7_PSHyHA/L_RKkfJI6bKu1M2~_mfesQ) ###

Creating a Refresh Token assigns a Workday user identity to an API Client to authorize
access to Workday Prism Analytics tables and data change tasks.  There can be many
refresh tokens for different Workday user associated with a single API Client.

From the _View API Clients_ task, on the API Clients for Integration tab, take the related 
action to Manage Refresh Tokens for Integrations.

![View API for Client Integrations](https://workday-prism-python.s3.amazonaws.com/ViewApiClients.png)

Refresh tokens always identify a Workday user.  In this example, 
the Refresh Token is for Logan Mcneil (lmcneil) and her security
groups and policies are applied to every REST API operation.

![Manage Refresh Token](https://workday-prism-python.s3.amazonaws.com/ManageRefreshToken.png)

After clicking the OK button, copy the Refresh Token.

![Refresh Token](https://workday-prism-python.s3.amazonaws.com/RefreshToken.png)

> **Note**: Refresh Tokens can be created, re-generated, and removed as often as
> necessary to identify the users allowed to use this API Client for Integration
> end point.

### [Obtain the Workday REST API Endpoint.](https://doc.workday.com/reader/J1YvI9CYZUWl1U7_PSHyHA/L_RKkfJI6bKu1M2~_mfesQ) ###

In Workday, obtain the Workday REST API base URL endpoint that the Prism class requires
as a parameter.  From the View API Client report, locate the base_url and tenant_name values.

![Base URL and Tenant Name](https://workday-prism-python.s3.amazonaws.com/URL-tenant.png)

### Configuration Summary ###

Before configuring the CLI or using the Prism client library, ensure you have the following values:

- Base URL
- Tenant Name
- Client ID
- Client Secret
- Refresh Token
- Log File Name (optional)
- Log Level (optional)

## Python Prerequisites ##

First determine whether you have an up-to-date versions of Python, 
pip, and Git.  If you need to install Python and Git, please refer
to the official installation guides.

### Python ###
Python comes preinstalled on most Linux distributions, and is available
as a package on all others. You can check which version of Python (if any)
is installed, by entering the following command in a terminal or command window:

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

**pip** is the preferred installer program. Starting with Python 3.4, it is included 
by default with the Python binary installers. You can check if pip is already installed
and up-to-date by entering the following command:

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

The installation instructions below use the Git client to retrieve
and install the Prism-Python package automatically. You can also retrieve this
package using your Git preferred method.

Before installing Git, you should first determine whether you already have it 
installed by running the following git command:

```bash
[user@host ~]$ git --version
git version 2.40.1
```

## Prism-Python Install ##

To automatically retrieve and install the latest version of this
package directly GitHub, use the following command:

```bash
pip install git+https://github.com/Workday/prism-python.git
```

It is also possible to install a specific tagged release with:

```bash
pip install git+https://github.com/Workday/prism-python.git@0.2.0
```

When either of these installations commands complete, the **prism** command
is available in your shell and provides access to the CLI.  For 
example, after installation the following command returns 
help on the available commands:

```bash
prism --list
```

## Prism-Python Configuration ##

The Python client library and CLI require the security credentials from the target
Workday Prism-enabled tenant, as well as other operational options.  For the Python 
client library, these options must be supplied when the Python client library object
is created.

```
import prism

pClientLib = prism.Prism(
    base_url=<my base url>,
    tenant_name=<my_tenant_name>,
    client_id=<my_client_id>,
    client_secret=<my_client_secret>,
    refrest_token=<my_refresh_token>
)
```

For the command line, the options can be set in three ways:

| Configuration         | Description                                                          |
|-----------------------|----------------------------------------------------------------------|
| Command Line          | Specified for each CLI operation.                                    |
| Environment Variables | Set in the operating system environment and used for CLI operations. |
| Configuration File    | One or more configurations stored in a file.                         |

When multiple configurations are available, i.e., specified on the command line 
and as environment variables, and in a configuration file the first instance of
an option is used, e.g., command line used before environment variables and environment
variables are used before configuration file values.

The following configuration options should be available: 

| Configuration | Description                                                                                                     |
|---------------|-----------------------------------------------------------------------------------------------------------------|
| base_url      | The service endpoint for the Workday tenant.                                                                    |
| tenant_name   | The tenant_name available at the service endpoint.                                                              |
| client_id     | The API Client for Integration ID created using the _Register API Client for Integration_ task.                 |
| client_secret | The API Client for Integration Secret created using the _Register API Client for Integration_ task.             |
| refresh_token | The Refresh Token for a Workday user created with the _Maintain Refresh Tokens for Integration_ related action. |
| config_file   | The name of a file containing configuration options. The default name is prism.ini.                             |
| config_name   | The name of a configuration section in the config_file.  The [default] section is used if not specified.        |
| log_file      | The name of a log file to capture information about the operation of the client library and CLI.                |
| log_level     | The output logging level, the default is INFO.  To see more information, set the value to DEBUG.                |

### Using Command line options ###

Command line options are always used regardless of other configurations and should appear **before** 
the CLI command. 

 ```bash
prism --base_url=<my base url> \
       --tenant_name <my tenant name> \
       --client_id <my client id> \
       --client_secret <my client secret> \
       --refresh_token <my refresh token> \
       tables get
```

### Using Environment variables ###

Set these options using operating specific commands.  For example, the following commands
set the environment variables in a Bash shell:

```bash
export workday_base_url=<my base url>
export workday_tenant_name=<my tenant name>
export prism_client_id=<my client id>
export prism_client_secret=<my client secret>
export prism_refresh_token=<my refresh token>
export prism_log_file=<my log file>
export prism_log_level=INFO

prism tables get
```

### Using a Configuration file ###

The CLI automatically looks for the file ``prism.ini`` in the current directory, and if found
reads configuration options from a section, by default the **[default]** section.  Use the 
``--config_name`` option to select a configuration other than **[default]**.

> **NOTE**: The client secrets and refresh tokens are the same as passwords and should be protected.

```ini
[default]
workday_base_url = https://<service url>
workday_tenant_name = <tenant name>
prism_client_id = MTFxx...MGI3ZWYx
prism_client_secret = cxxxx...vsanmq
prism_refresh_token = weyyyyy...boakc
prism_log_level = INFO

[integration]
workday_base_url = https://<service url>
workday_tenant_name = <integration tenant name>
prism_client_id = NTFmx...MmZlYmUy
prism_client_secret = qnnnn...3f4kkmi
prism_refresh_token = jtqqqq...qfcm9s5
prism_log_level = INFO
```

```bash
prism --config_file myconfig.ini \
      --config_name integration \
      tables get
```

## Python client library example

### Create a new Prism table
The following Python script uses the Prism-Python client library to create
a new Workday Prism Analytics Table and load the contents of a delimited
and compressed CSV file (.csv.gz).

```
import os
import prism

# STEP 1 - Initialize the Prism-Python client library using
#          environment variables.
pClientLib = prism.Prism(
    os.getenv("workday_base_url"),
    os.getenv("workday_tenant_name"),
    os.getenv("prism_client_id"),
    os.getenv("prism_client_secret"),
    os.getenv("prism_refresh_token")
)

# STEP 2 - Create a new table using the definition of fields provided 
#          by the schema.json file.
table = prism.tables_create(
    p=pClientLib,
    table_name="my_new_table",
    file="/path/to/schema.json"
)

# Print JSON response body describing the new table.
print(table)

# STEP 3 - Use the convenience function prism.upload_file() to upload 
#          a local file to the table.  Notice the "operation" is Insert 
#          for the first load.
prism.upload_file(
    p=pClientLib,
    file="/path/to/data.csv.gz", 
    table_id=table["id"],
    operation="Insert"
)
```

### Manage data in an existing table

The Workday Prism REST API provides multiple operations for adding,
updating and removing data from a Workday Prism Analytics table.  One of
the following table operations must be specified for a loading operation.

- **Insert**: Workday keeps any existing data in the target table 
and adds new data from the source.
- **TruncateAndInsert**: Workday deletes all existing data
in the target table and replaces it with data from the source.
- **Delete**: Workday deletes data from the target table 
based external ID data from the source.
- **Update**: Workday updates only existing data in the 
target table based on data from the source.  All matching rows,
based on the external ID value, are updated.
- **Upsert**: Workday inserts new data from the source if it 
doesn't exist in the target table, and updates existing data
using the external ID value from the source to locate matching
rows.

When using a `Delete`, `Update`, or `Upsert` operation, the source data
must contain an ``externalId`` attribute matching the ``externalId``
defined in the target table, i.e., a primary key value.

```
# STEP 4 - Use the prism.upload convenience function
# to truncate the existing data and load new data
# from two CSV files.
prism.upload_file(
    pClientLib, 
    ["/path/to/newdata-1.csv", "/path/to/newdata-2.csv"], 
    table["id"], 
    operation="TruncateAndInsert"
)
```

Note: the Workday Prism Analytics REST API only accepts delimited
and gzip compressed (.csv.gz) files.  The ``upload`` convenience
function automatically performs the gzip operation.

## CLI Example

The command line interface (CLI) is a no-code way to interact with
the Workday Prism Analytics REST API. The CLI expects tenant and credential
options to be passed on the command line, stored as environment variables, 
or stored in a configuration file (see Configuration section above).

For the following examples, a ``prism.ini`` exists in the current 
working directory with a ``[default]`` section.

```bash
# Get help with the CLI.
[ user@host]$ prism --help

# Get help for the tables command.
[ user@host]$ prism tables --help

# Use the Workday Prism Analytics REST API GET:/tables endpoint 
# to list Prism tables you have access to.
[ user@host]$ prism tables get

# Create a new Prism Table using the Workday Prism Analytics 
# REST API POST:/tables endpoint
[ user@host]$ prism tables create my_new_table /path/to/schema.json

# Upload data to the new table using the ID value - the default
# table operation is "TruncateAndInsert"
[ user@host]$ prism tables upload 83dd72bd7b911000ca2d790e719a0000 /path/to/file1.csv.gz

# Upload multiple CSV files to a Prism API table.  Notice the --isName (-n)
# option tells the CLI to lookup the table id.
[ user@host]$ prism tables upload \
                 -operation Insert \
                 -isName my_new_table \
                 /path/to/*.csv
```

## Bugs
Please report any bugs that you find [here](https://github.com/Workday/prism-python/issues).
Or, even better, fork the repository on [GitHub](https://github.com/Workday/prism-python)
and create a pull request (PR). We welcome all changes, big or small, and we
will help you make the PR if you are new to `git`.

## License
Released under the Apache-2.0 license (see [LICENSE](LICENSE))
