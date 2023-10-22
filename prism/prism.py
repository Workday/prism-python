"""
Load data into Workday Prism Analytics.

The Prism API provides a flexible, secure and scalable way to load data into
Workday Prism Analytics.

DocString style: https://www.sphinx-doc.org/en/master/usage/extensions/example_numpy.html
"""

import logging
import json
import requests
import time
import os
import sys
import uuid
import io
import gzip
import inspect

from urllib import parse as urlparse

# Default a logger - the default may be re-configured in the set_logging method.
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# writing to stdout only...
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.WARNING)
log_format = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(log_format)
logger.addHandler(handler)


def set_logging(log_file=None, log_level="INFO"):
    """

    :param log_file:
    :param log_level:
    :return:
    """
    # Resolve the log level - default to info if empty or invalid.
    if log_level is None:
        set_level = logging.INFO
    else:
        # Make sure the caller gave us a valid "name" (INFO/DEBUG/etc) for logging level.
        if hasattr(logging, log_level):
            set_level = getattr(logging, log_level)
        else:
            set_level = logging.INFO

    # If no file was specified, simply loop over any handlers and
    # set the logging level.
    if log_file is None:
        logger.setLevel(set_level)

        for log_handler in logger.handlers:
            log_handler.setLevel(set_level)
    else:
        # Setup logging for CLI operations.
        for log_handler in logger.handlers:
            logger.removeHandler(log_handler)

        logger.setLevel(set_level)

        # Create a handler as specified by the user (or defaults)
        fh = logging.FileHandler(log_file)
        fh.setLevel(set_level)

        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)

        logger.addHandler(fh)

    logger.debug(f"set log level: {set_level}")


def log_elapsed(msg, timedelta):
    """Log the elapsed time of a get/post/put/patch HTTP operation."""
    elapsed = timedelta.total_seconds()
    logger.debug(f"{msg}: elapsed {elapsed:.5f}")


def buckets_gen_name():
    bucket_name = "cli_" + uuid.uuid4().hex
    logger.debug(f"buckets_gen_name: created bucket name: {bucket_name}")

    return bucket_name


def schema_fixup(schema):
    """Utility function to revise a schema for a bucket operations."""

    if schema is None:
        logger.error("schema_fixup: schema cannot be None.")
        return False

    if not isinstance(schema, dict):
        logger.error("schema_fixup: schema is not a dictionary.")
        return False

    def is_valid_string(attr):
        if attr not in schema or not isinstance(schema[attr], str) or len(schema[attr]) == 0:
            return False

        return True

    def is_valid_list(attr):
        if attr not in schema or not isinstance(schema[attr], list):
            return False

        return True

    if not is_valid_string('id'):
        logger.error("id attribute missing")
        return False

    if not is_valid_list('fields'):
        logger.error("fields attribute missing from schema!")
        return False

    # Remove Prism managed fields "WPA_*"
    schema['fields'] = [fld for fld in schema['fields'] if not fld['name'].startswith('WPA_')]

    # Add a sequential order (ordinal) on the fields to (en)force
    # required sequencing of fields.
    for ordinal in range(len(schema["fields"])):
        fld = schema["fields"][ordinal]
        fld["ordinal"] = ordinal + 1

    keys = list(schema.keys())

    for k in keys:
        if k not in ['name', 'id', 'fields', 'tags', 'displayName', 'description', 'documentation',
                     'enableForAnalysis']:
            del schema[k]

    return True


def table_to_bucket_schema(table):
    """Convert schema derived from list table to a bucket schema.

    Parameters
    ----------
    table: dict
        A dictionary containing the schema definition for your dataset.

    Returns
    -------
    If the request is successful, a dictionary containing the bucket schema is returned.
    The results can then be passed to the create_bucket function

    """

    # describe_schema is a python dict object and needs to be accessed as such, 'data' is the top level object,
    # but this is itself a list (with just one item) so needs the list index, in this case 0. 'fields' is found
    # in the dict that is in ['data'][0]

    if table is None or "fields" not in table:
        logger.error("Invalid table passed to table_to_bucket_schema.")
        return None

    bucket_schema = {
        "schemaVersion": {"id": "Schema_Version=1.0"},
    }

    fields = table["fields"]

    # Get rid of any WPA_ fields...
    fields[:] = [x for x in fields if "WPA" not in x["name"]]

    # Create and assign useAsOperationKey field with true/false values based on externalId value
    operation_key_false = {"useAsOperationKey": False}
    operation_key_true = {"useAsOperationKey": True}

    for fld in fields:
        if fld["externalId"] is True:
            fld.update(operation_key_true)
        else:
            fld.update(operation_key_false)

    # Now trim our field attributes to keep just what we need
    for fld in fields:
        for attr in ['id', 'displayName', 'fieldId', 'required', 'externalId']:
            if attr in fld:
                del fld[attr]

    if 'parseOptions' in table:
        bucket_schema['parseOptions'] = table['parseOptions']
    else:
        bucket_schema['parseOptions'] = {
            "fieldsDelimitedBy": ",",
            "fieldsEnclosedBy": '"',
            "headerLinesToIgnore": 1,
            "charset": {"id": "Encoding=UTF-8"},
            "type": {"id": "Schema_File_Type=Delimited"},
        }

    # Build the final bucket definition.
    bucket_schema['fields'] = fields

    return bucket_schema


class Prism:
    """Class for interacting with the Workday Prism API.


    Attributes:
        base_url (str): URL for the Workday API client
        tenant_name (str): Workday tenant name
        client_id (str): Client ID for the registered API client
        client_secret (str): Client Secret for the registered API client
        refresh_token (str): Refresh Token for the Workday user
        version (str): Version of the Prism API to use
    """

    def __init__(self, base_url, tenant_name, client_id, client_secret, refresh_token, version="v3"):
        """Init the Prism class with required attributes."""

        # Capture the arguments into the class variables.
        self.base_url = base_url
        self.tenant_name = tenant_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.version = version

        # Compose the endpoints for authentication and API calls.
        self.token_endpoint = f"{base_url}/ccx/oauth2/{tenant_name}/token"
        self.rest_endpoint = f"{base_url}/ccx/api/{version}/{tenant_name}"
        self.prism_endpoint = f"{base_url}/api/prismAnalytics/{version}/{tenant_name}"
        self.upload_endpoint = f"{base_url}/wday/opa/tenant/{tenant_name}/service/wBuckets"

        # Support URLs for additional Workday API calls.
        self.wql_endpoint = f"{base_url}/api/wql/v1/{tenant_name}"
        """str: Workday Report as a Service (raas) endpoint."""

        self.raas_endpoint = f"{base_url}/ccx/service"
        """str: Workday Report as a Service (raas) endpoint."""

        # At creation, there cannot yet be a bearer_token obtained from Workday.
        self.bearer_token = None
        """str: Active bearer token for the session."""

        self.bearer_token_timestamp = None
        """time.time: Last bearer token time."""

        # Helper constants.
        self.CONTENT_APP_JSON = {"Content-Type": "application/json"}
        self.CONTENT_FORM = {"Content-Type": "application/x-www-form-urlencoded"}

    def http_get(self, url, headers=None, params=None):
        """Pass the headers and params to the URL to retrieve

        :param url:
        :param headers:
        :param params:
        :return:
        """
        caller = inspect.stack()[1][3]
        logger.debug(f"get: called by {caller}")

        if url is None or not isinstance(url, str) or len(url) == 0:
            # Create a fake response object for standard error handling.
            msg = "get: missing URL"

            response = {"status_code": 600,
                        "text": msg,
                        "errors": [{"error": msg}]}
        else:
            logger.debug(f"get: {url}")

            # Every request requires an authorization header - make it true.
            if headers is None:
                headers = {}

            if "Authorization" not in headers:
                headers["Authorization"] = "Bearer " + self.get_bearer_token()

            response = requests.get(url, params=params, headers=headers)
            log_elapsed(f"get: {caller}", response.elapsed)

        if response.status_code != 200:
            logger.error(f"Invalid HTTP status: {response.status_code}")
            logger.error(f"Reason: {response.reason}")
            logger.error(f"Text: {response.text}")

        return response

    def http_post(self, url, headers=None, data=None, files=None):
        caller = inspect.stack()[1][3]
        logger.debug(f"post: called by {caller}")

        if url is None or not isinstance(url, str) or len(url) == 0:
            # Create a fake response object for standard error handling.
            msg = "POST: missing URL"

            response = {"status_code": 600,
                        "text": msg,
                        "errors": [{"error": msg}]}
        else:
            logger.debug(f"post: {url}")

            # Every request requires an authorization header - make it true.
            if headers is None:
                headers = {}

            if "Authorization" not in headers and caller != "create_bearer_token":
                headers["Authorization"] = "Bearer " + self.get_bearer_token()

            response = requests.post(url, headers=headers, data=data, files=files)
            log_elapsed(f"put: {caller}", response.elapsed)

        if response.status_code > 299:
            logger.error(response.text)

        return response

    def http_patch(self, url, headers=None, data=None):
        caller = inspect.stack()[1][3]
        logger.debug(f"patch: called by {caller}")

        if url is None or not isinstance(url, str) or len(url) == 0:
            # Create a fake response object for standard error handling.
            msg = "PATCH: missing URL"

            response = {"status_code": 600,
                        "text": msg,
                        "errors": [{"error": msg}]}
        else:
            logger.debug(f"patch: {url}")

            # Every request requires an authorization header - make it true.
            if headers is None:
                headers = {}

            if "Authorization" not in headers and caller != "create_bearer_token":
                headers["Authorization"] = "Bearer " + self.get_bearer_token()

            response = requests.patch(url, headers=headers, data=json.dumps(data))
            log_elapsed(f"patch: {caller}", response.elapsed)

        if response.status_code > 299:
            logger.error(response.text)

        return response

    def http_put(self, url, headers=None, data=None):
        caller = inspect.stack()[1][3]
        logger.debug(f"put: called by {caller}")

        if url is None or not isinstance(url, str) or len(url) == 0:
            # Create a fake response object for standard error handling.
            msg = "PUT: missing URL"

            response = {"status_code": 600,
                        "text": msg,
                        "errors": [{"error": msg}]}
        else:
            logger.debug(f"put: {url}")

            # Every request requires an authorization header - make it true.
            if headers is None:
                headers = {}

            if "Authorization" not in headers and caller != "create_bearer_token":
                headers["Authorization"] = "Bearer " + self.get_bearer_token()

            if "Content-Type" not in headers:
                headers["Content-Type"] = 'application/json'

            response = requests.put(url, headers=headers, data=json.dumps(data))
            log_elapsed(f"put: {caller}", response.elapsed)

        if response.status_code > 299:
            logger.error(response.text)

        return response

    def create_bearer_token(self):
        """Exchange a refresh token for an access token.

        Parameters
        ----------

        Returns
        -------
        If the request is successful, the access token is added to the Prism()
        class.

        """

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        r = self.http_post(url=self.token_endpoint, headers=self.CONTENT_FORM, data=data)

        if r.status_code == 200:
            logger.debug("successfully obtained bearer token")
            self.bearer_token = r.json()["access_token"]
            self.bearer_token_timestamp = time.time()
        else:
            logger.error(f"create bearer token failed: HTTP status code.")
            self.bearer_token = None
            self.bearer_token_timestamp = None

    def get_bearer_token(self):
        """Get the current bearer token, or create a new one

        Note:
            If the token doesn't exist, or it's older than 15 minutes create
            a new token.

        Returns:
            Workday bearer token.
        """
        if self.bearer_token is None or (time.time() - self.bearer_token_timestamp) > 900:
            self.create_bearer_token()

        if self.bearer_token is None:
            return ""  # Only return strings

        return self.bearer_token

    def reset_bearer_token(self):
        """Reset the current bearer token to none.

        Note: Use this to force getting a new token on the next API call.
        """
        self.bearer_token = None
        self.bearer_token_timestamp = None

    def tables_get(
            self,
            name=None, id=None,
            limit=None, offset=None,
            type_="summary",
            search=False):
        """Obtain details for all tables or a given table(s).

        Notes
        -----
            This method never fails and always returns a valid Dict.

        Parameters
        ----------
        name : str
            The name of the table to obtain details about. If the default value
            of None is specified.
        id : str
            The ID of a table to obtain details about.  When specified, all tables
            are searched for the matching id.
        limit : int
            The maximum number of tables to be queried, if None all tables are returned.
        offset: int
            The offset from zero of tables to return.
        type_ : str
            Level of detail to return.
        search : bool
            Enable contains searching for table names and display names.

        Returns
        -------
        dict
            For an ID query, return the table information as a dict.  For any other
            table list query, return a total attribute of the number of tables found and data
            attribute containing the list tables.
        """
        operation = "/tables"

        if type_ is None or type_.lower() not in ["full", "summary", "permissions"]:
            logger.warning("Invalid output type for tables list operation - defaulting to summary.")
            output_type = "summary"
        else:
            output_type = type_.lower()

        # If we got a WID, then do a direct query by ID - no paging or searching required.
        if id is not None:
            operation = f"{operation}/{id}?format={output_type}"
            logger.debug(f"get: {operation}")
            url = self.prism_endpoint + operation

            response = self.http_get(url)

            if response.status_code == 200:
                return response.json()
            else:
                return None

        # We are doing a query by attributes other than ID.
        logger.debug(f"get: {operation}")
        url = self.prism_endpoint + operation

        # Always return a valid JSON object of results regardless of
        # errors or API responses.  THIS METHOD NEVER FAILS.
        return_tables = {"total": 0, "data": []}

        # Start setting up the API call parameters.
        params = {
            'limit': limit if limit is not None else 100,
            'offset': offset if offset is not None else 0,
            'type': output_type
        }

        # See if we want to add an explicit table name as a search parameter.
        if not search and name is not None:
            # Here, the caller is not searching, they gave us an exact name.
            params["name"] = name.replace(" ", "_")  # Minor clean-up

            # Should only be 0 (not found) or 1 (found) tables found.
            params['limit'] = 1
            params['offset'] = 0

        # If we didn't get a limit, turn on searching to retrieve all tables.
        if limit is None:
            search = True  # Force a search so we get all tables

            params["limit"] = 100  # Max pagesize to retrieve in the fewest REST calls.
            params["offset"] = 0

        # Always assume we will retrieve more than one page.
        while True:
            r = self.http_get(url, params=params)

            if r.status_code != 200:
                # Whatever we have captured (perhaps zero tables) so far
                # will be returned due to unexpected status code.  Break
                # and do final clean-up on exit.
                break

            # Convert the response to a list of tables.
            tables = r.json()

            if not search and name is not None:  # Explicit table name
                # We are not searching, and we have a specific table - return
                # whatever we got (maybe nothing).
                return tables

            # Figure out what tables of this batch of tables should be part of the
            # return results, i.e., search the this batch for matches.
            if name is not None:
                # Substring search for matching table names, display names
                match_tables = [tab for tab in tables["data"]
                                if name.lower() in tab["name"].lower() or name.lower() in tab["displayName"].lower()]
            else:
                # Grab all the tables in the result
                match_tables = tables["data"]

            return_tables["data"] += match_tables

            # If we get back anything but a full page, we are done
            # paging the results.
            if len(tables["data"]) < params['limit']:
                break

            if search:
                # Move on to the next page.
                params['offset'] += params['limit']
            else:
                # The caller asked for a specific limit and offset, exit the loop.
                break

        # We always return a dict with the total tables found.
        return_tables['total'] = len(return_tables['data'])  # Separate step for debugging.
        return return_tables

    def tables_post(self, schema):
        """Create an empty table of type "API".

        Parameters
        ----------
        schema : dict
            A dictionary containing the schema

        Returns
        -------
        dict
            If the request is successful, a dictionary containing information about
            the new table is returned, otherwise None.
        """
        operation = "/tables"
        logger.debug(f"POST : {operation}")
        url = self.prism_endpoint + "/tables"

        if not schema_fixup(schema):
            logger.error("Invalid schema for create operation.")
            return None

        response = self.http_post(url=url, headers=self.CONTENT_APP_JSON, data=json.dumps(schema))

        if response.status_code == 201:
            return response.json()

        return None

    def tables_put(self, id, schema, truncate=False):
        """Update an existing table using a full schema definition.

        Notes
        -----
        For certain changes, e.g., changing a data type, the table cannot
        have any data.

        Parameters
        ----------
        id : str
            Prism Table ID of an existing table.

        schema : dict
            A dictionary containing the schema

        truncate : bool
            True to automatically truncate the table before
            applying the new schema.

        Returns
        -------
        dict
            If the request is successful, a dictionary containing information about
            the new table is returned, otherwise None.
        """
        operation = f"/tables/{id}"
        logger.debug(f"PUT: {operation}")
        url = self.prism_endpoint + operation

        if not schema_fixup(schema):
            logger.error("Invalid schema for update operation.")
            return None

        response = self.http_put(url=url, data=schema)

        if response.status_code == 200:
            return response.json()

        return None

    def tables_patch(self, id, patch):
        """Patch the table with specified values.

        Notes
        -----
            Patching only changes a short list of table
            level attributes.

        Parameters
        ----------
        id : str
            Prism Table ID of an existing table.

        patch : dict
            One or more table attributes to update.

        Returns
        -------
        dict
            If the request is successful, a dictionary containing information about
            the new table is returned, otherwise None.
        """
        operation = f'/tables/{id}'
        logger.debug(f'PATCH: {operation}')
        url = self.prism_endpoint + operation

        response = self.http_patch(url=url, headers=self.CONTENT_APP_JSON, data=patch)

        if response.status_code == 200:
            return response.json()

        return None

    def buckets_get(self,
                    id=None, name=None,
                    limit=None, offset=None,
                    type_="summary",
                    table_name=None, search=False):
        """Get a one or more bucket definitions.

        Parameters
        ----------
        id : str
            The ID of an existing bucket.
        name : str
            The name of an existing bucket.
        limit : int
            The maximum number of tables to be queried, if None all tables are returned.
        offset: int
            The offset from zero of tables to return.
        type_ : str
            Level of detail to return.
        table_name : str
            List all/any buckets for associated with the table name.
        search : bool
            Enable contains searching for bucket names and display names.

        Returns
        -------
        dict
            For an ID query, return the bucket information as a dict.  For any other
            bucket query, return a total attribute of the number of buckets found and data
            attribute containing the list buckets.
        """
        operation = "/buckets"

        output_type = type_.lower() if type_.lower() in ['full', 'summary'] else 'summary'

        # If we got an ID, then do a direct query by ID - no paging or searching required.
        if id is not None:
            operation = f"{operation}/{id}?format={output_type}"
            logger.debug(f"get: {operation}")
            url = self.prism_endpoint + operation

            response = self.http_get(url)

            if response.status_code == 200:
                return response.json()
            else:
                return None

        logger.debug(f"get: {operation}")
        url = self.prism_endpoint + operation

        # Start the return object - this routine NEVER fails
        # and always returns a valid dict object.
        return_buckets = {"total": 0, "data": []}

        params = {
            'limit': limit if limit is not None else 100,
            'offset': offset if offset is not None else 0,
            'type': output_type
        }

        if not search and name is not None:
            # List a specific bucket name overrides any other
            # combination of search/table/bucket name/wid.
            params['name'] = name

            params['limit'] = 1  # Can ONLY be one matching bucket.
            params['offset'] = 0
        else:
            # Any other combination of parameters requires a search
            # through all the buckets in the tenant.
            search = True

            params['limit'] = 100  # Max pagesize to retrieve in the fewest REST calls.
            params['offset'] = 0

        while True:
            r = self.http_get(url, params=params)

            if r.status_code != 200:
                # This routine never fails, return whatever we got (if any).
                break

            buckets = r.json()

            if not search and name is not None:  # Explicit bucket name
                # We are not searching, and we have a specific bucket,
                # return whatever we got with this call (it will be in
                # the necessary dict structure).
                return buckets

            if name is not None:  # We are searching at this point.
                # Substring search for matching table names
                match_buckets = [bck for bck in buckets["data"] if
                                 name in bck["name"] or name in bck["displayName"]]
            elif table_name is not None:
                # Caller is looking for any/all buckets by target table
                match_buckets = [
                    bck for bck in buckets["data"]
                    if table_name == bck["targetDataset"]["descriptor"] or
                    (search and table_name.lower() in bck["targetDataset"]["descriptor"].lower())
                ]
            else:
                # Grab all the tables in the result - select all buckets.
                match_buckets = buckets["data"]

            # Add to the results.
            return_buckets["data"] += match_buckets

            # If we get back a list of buckets fewer than a full page, we are done
            # paging the results.
            if len(buckets["data"]) < params["limit"]:
                break

            if search:
                # Move on to the next page...
                params["offset"] += params["limit"]
            else:
                # The caller asked for a specific limit and offset, exit the loop.
                break

        # We always return a valid count of buckets found.
        return_buckets["total"] = len(return_buckets["data"])

        return return_buckets

    def buckets_create(
            self,
            name=None,
            target_name=None,
            target_id=None,
            schema=None,
            operation="TruncateAndInsert"):
        """Create a Prism bucket to upload files.

        Notes
        -----
            A table name (without a table id) retrieves the table id.

            Default operation is TruncateAndInsert, valid operations include
            “Insert”, “Update”, “Upsert” and “Delete”

            For Update/Upsert/Delete operations, one field in the table must have the
            ‘useAsOperationKey’ attribute set to True. Only fields marked as ExternalID
            or WPA_RowID or WPA_LoadId on Table schema can be used as operation keys
            during loads into the table.

        Parameters
        ----------
        name : str
            Name of the bucket to create, default to a new generated name.
        target_id : str
            The ID of the table for this bucket.
        target_name : str
            The name of the table for bucket.
        schema : dict
            A dictionary containing the schema for your table.
        operation : str
           Required, defaults to "TruncateAndInsert" operation

        Returns
        -------
        dict
            Information about the new bucket, or None if there was a problem.
        """

        # If the caller didn't give us a name to use for the bucket,
        # create a default name.
        if name is None:
            bucket_name = buckets_gen_name()
        else:
            bucket_name = name

        table_schema = None
        bucket_schema = None

        if schema is not None:
            if isinstance(schema, dict):
                table_schema = schema
            elif isinstance(schema, str):
                try:
                    with open(schema) as schema_file:
                        table_schema = json.load(schema_file)
                except Exception as e:
                    logger.error(e)
                    return None
            else:
                logger.error('invalid schema expecting dict or file name.')
                return None

        # Resolve the target table; if specified.
        if target_id is None and target_name is None:
            if table_schema is None:
                logger.error("schema, target id or target name is required to create a bucket.")
                return None

            if 'id' not in table_schema or 'fields' not in table_schema:
                logger.error('schema missing "id" or "fields" attribute.')
                return None
        else:
            if target_id is not None:  # Always use ID if provided.
                table = self.tables_get(id=target_id, type_="full")  # Full=include fields object

                if table is None:
                    logger.error(f'table ID {target_id} not found.')
                    return None
            else:
                tables = self.tables_get(name=target_name, type_="full")

                if tables["total"] == 0:
                    logger.error(f"table not found for bucket operation.")
                    return None

                table = tables['data'][0]

            if table_schema is None:
                table_schema = table
            else:
                # Override the definition of the table in the schema.
                table_schema['id'] = table['id']

        # We have the table and the user didn't include a schema. Make a copy
        # of the target table's schema.
        if not schema_fixup(table_schema):
            logger.error('Invalid schema for bucket operation.')
            return None

        bucket_schema = table_to_bucket_schema(table_schema)

        logger.debug(f"post: /buckets")
        url = self.prism_endpoint + "/buckets"

        data = {
            "name": bucket_name,
            "operation": {"id": "Operation_Type=" + operation},
            "targetDataset": {"id": table_schema["id"]},
            "schema": bucket_schema,
        }

        response = self.http_post(url, headers=self.CONTENT_APP_JSON, data=json.dumps(data))

        if response.status_code == 201:
            logger.info("successfully created a new wBucket")
            return response.json()

        return None

    def buckets_complete(self, id):
        """
        Commit the data contained in the bucket to the associated table.

        Parameters
        ----------
        id : str
            The ID of an existing bucket with a "New" status.

        Returns
        -------
        dict
            Information about the completed bucket, or None if there was a problem.
        """
        operation = f'/buckets/{id}/complete'
        logger.debug(f'post: {operation}')
        url = self.prism_endpoint + operation

        r = self.http_post(url)

        if r.status_code == 201:
            logger.info(f'successfully completed wBucket {id}.')
            return r.json()

        return None

    def buckets_files(self, id, file=None):
        """Upload a file to a given bucket.

        Notes
        -----
            The file may be a single file or a list of files having
            and extension of .CSV or .CSV.GZ (lowercase).

            When a .CSV file is encountered, automatically GZIP before
            uploading.

        Parameters
        ----------
        id : str
            Upload the file to the bucket identified by ID.

        file : str | list(str)
            The file(s) to upload to the bucket. Each file must conform
            to the file size limits.

        Returns
        -------
            Upload information or None if there was a problem.  When uploading
            multiple files, an array of upload information with information for
            each file.
        """
        operation = f"/buckets/{id}/files"
        logger.debug("post: {operation}")
        url = self.prism_endpoint + operation

        results = {'total': 0, 'data': []}  # Always return a valid list - regardless of files

        if file is None:
            # It is legal to upload an empty file - see the table truncate command.
            target_files = [None]  # Provide one empty file to iterate over.
        else:
            target_files = resolve_file_list(file)

        target_file: str
        for target_file in target_files:
            if target_file is None:
                new_file = {"file": ("empty", io.BytesIO())}
            elif target_file.lower().endswith(".csv.gz"):
                new_file = {"file": open(target_file, "rb")}
            elif target_file.lower().endswith(".csv"):
                upload_filename = os.path.basename(target_file)
                upload_filename += ".gz"

                # Buckets can only load gzip files - do it.
                with open(target_file, "rb") as in_file:
                    new_file = {"file": (upload_filename, gzip.compress(in_file.read()))}

            response = self.http_post(url, files=new_file)

            if response.status_code == 201:
                logger.debug(f"successfully uploaded {target_file} to the bucket")

                results['data'].append(response.json())  # Add this file's info to the return list

        results['total'] = len(results['data'])
        return results

    def buckets_errorFile(self, id):
        """Get a list of all rows that failed to load into the table

        Parameters
        ----------
        id : str
             A reference to a Prism Analytics bucket.

        Returns
        -------
        str
        """

        if id is None:
            logger.error('bucket id is required.')
            return None

        operation = f"/buckets/{id}/errorFile"
        logger.debug("post: {operation}")
        url = self.prism_endpoint + operation

        response = self.http_get(url)

        if response.status_code == 200:
            return response.text

        return None

    def dataChanges_get(self,
                        name=None, id=None,
                        limit=None, offset=None,
                        type_='summary', search=False,
                        refresh=False):
        """
        """
        # We are doing a dataChanges GET operation.
        operation = "/dataChanges"

        # Make sure output type is valid.
        output_type = type_.lower() if type_.lower() in ['summary', 'full'] else 'summary'

        # Searching by ID is a special case that eliminates all other types
        # of search.  Ask for the datachange by id and return just this
        # result - even blank.
        if id is not None and isinstance(id, str) and len(id) > 0:
            operation = f"{operation}/{id}?type={output_type}"
            logger.debug(f'dataChanges_get: {operation}')
            url = self.prism_endpoint + operation

            response = self.http_get(url)

            if response.status_code == 200:
                return response.json()

            return None

        logger.debug(f"dataChanges_get: {operation}")
        url = self.prism_endpoint + operation

        # Get a list of tasks by page, with or without searching.

        search_limit = 500  # Assume all DCTs should be returned - max API limit
        search_offset = 0  # API default value

        if limit is not None and isinstance(limit, int) and limit > 0:
            search_limit = limit

        if offset is not None and isinstance(offset, int) and offset > 0:
            search_offset = offset

        searching = False
        name_param = ""

        if name is not None and isinstance(name, str) and len(name) > 0:
            if search is not None and isinstance(search, bool) and search:
                # Force a return of ALL data change tasks, so we can search the names.
                name_param = ""
                searching = True

                search_limit = 500
                search_offset = 0
            else:
                # With an explicit name, we should return at most 1 result.
                name_param = "&name=" + urlparse.quote(name)
                searching = False

                search_limit = 1
                search_offset = 0

        # Assume we will be looping based on limit and offset values; however, we may
        # execute only once.  NOTE: this routine NEVER fails, but may return zero
        # data change tasks.

        data_changes = {"total": 0, "data": []}

        while True:
            search_url = f"{url}?type={output_type}&limit={search_limit}&offset={search_offset}{name_param}"
            logger.debug(f"dataChangesID url: {search_url}")

            response = self.http_get(url=search_url)

            if response.status_code != 200:
                break

            return_json = response.json()

            if searching:
                # Only add matching rows
                data_changes["data"] += \
                    filter(lambda dtc: dtc["name"].find(name) != -1 or
                           dtc["displayName"].find(name) != -1,
                           return_json["data"])
            else:
                # Without searching, simply paste the current page to the list.
                data_changes["data"] += return_json["data"]
                break

            # If we didn't get a full page, then we are done.
            if len(return_json["data"]) < search_limit:
                break

            # Go to the next page.
            offset += search_limit

        data_changes["total"] = len(data_changes["data"])

        return data_changes

    def dataChanges_activities_get(self, id, activityID):
        """Returns details of the activity specified by activityID.

        Parameters
        ----------
        id : str
             A reference to a Prism Analytics data change.
        """
        operation = f"/dataChanges/{id}/activities/{activityID}"
        logger.debug(f"dataChanges_activities_get: {operation}")
        url = self.prism_endpoint + operation

        r = self.http_get(url)

        if r.status_code == 200:
            return r.json()

        return None

    def dataChanges_activities_post(self, id, fileContainerID=None):
        """Execute a data change task.

        Parameters
        ----------
        id : str
             A reference to a Prism Analytics data change.
        fileContainerID : str
            A reference to a Prism Analytics File Container.

        Returns
        -------
        """
        operation = f"/dataChanges/{id}/activities"
        logger.debug(f"post: {operation}")
        url = self.prism_endpoint + operation

        if fileContainerID is None:
            logger.debug("no file container ID")
            data = None
        else:
            logger.debug('with file container ID: {fileContainerID}')

            # NOTE: the name is NOT correct based on the API definition
            data = json.dumps({"fileContainerWid": fileContainerID})

        r = self.http_post(url, headers=self.CONTENT_APP_JSON, data=data)

        if r.status_code == 201:
            return_json = r.json()
            activity_id = return_json["id"]

            logger.debug(f"successfully started data load task - id: {activity_id}")
            return return_json
        elif r.status_code == 400:
            logger.error(f'error running data change task.')
            return r.json()  # This is still valid JSON with the error.

        return None

    def dataChanges_is_valid(self, id):
        """Utility method to return the validation status of a data change task.

        Parameters
        ----------
        id : str
             A reference to a Prism Analytics data change.

        Returns
        -------
        bool
            True if data change task is valid or False if the task does not
            exist or is not valid.
        """
        dct = self.dataChanges_validate(id)

        if dct is None:
            logger.error(f"data_change_id {id} not found!")
            return False

        if "error" in dct:
            logger.error(f"data_change_id {id} is not valid!")
            return False

        # There is no specific status value to check, we simply get
        # a small JSON object with the ID of the DCT if it is valid.
        return True

    def dataChanges_validate(self, id):
        """validates the data change specified by dataChangeID

        Parameters
        ----------
        id : str
            The data change task ID to validate.

        Returns
        -------
        """
        operation = f"/dataChanges/{id}/validate"
        logger.debug(f"dataChanges_validate: get {operation}")
        url = self.prism_endpoint + operation

        r = self.http_get(url)

        if r.status_code in [200, 400, 404]:
            # For these status codes, simply return what we got.
            return r.json()

        return None

    def dataExport_get(self, limit=None, offset=None, type_=None):
        operation = '/dataExport'
        logger.debug(f"dataExport_get: get {operation}")
        url = self.prism_endpoint + operation

        r = self.http_get(url)

        if r.status_code == 200:
            return r.json()

        return None

    def fileContainers_create(self):
        """Create a new file container.

        Returns
        -------
            Dict object with an "id" attribute or None if there was a problem.
        """
        operation = "/fileContainers"
        logger.debug(f"fileContainer_create: post {operation}")
        url = self.prism_endpoint + operation

        r = self.http_post(url)

        if r.status_code == 201:
            return_json = r.json()

            file_container_id = return_json["id"]
            logger.debug(f"successfully created file container: {file_container_id}")

            return return_json

        return None

    def fileContainers_get(self, id):
        """Return all files for a file container.

        Parameters
        ----------
        id : str
            File container ID to list.

        Returns
        -------
            Dictionary of found files having a "total" attribute with the count
            of files uploaded and a data attribute with an array of file metadata
            for each file in the container.
        """
        operation = f"/fileContainers/{id}/files"
        logger.debug(f"fileContainers_list: get {operation}")
        url = self.prism_endpoint + operation

        response = self.http_get(url)

        if response.status_code == 200:
            return_json = response.json()

            return {'total': len(return_json), 'data': return_json}

        if response.status_code == 404:
            logger.warning('verify: Self-Service: Prism File Container domain in the Prism Analytics functional area.')

        return {"total": 0, 'data': []}  # Always return a list.

    def fileContainers_load(self, id, file):
        """
        Load one or more files to a fileContainer.

        Parameters
        ----------
        id : str
            File container ID of target container.
        file : str|list
            File name(s) to load into the container

        Returns
        -------
            For a single file, the upload results are returned as a
            dict.  For multiple files, an array of results is returned.
        """

        # Create the specified fID - a new ID is created if None.
        resolved_fid = id

        target_files = resolve_file_list(file)

        results = {
            'id': None,
            'total': 0,
            'data': []
        }

        for target_file in target_files:
            # It is legal to upload an empty file - see the table truncate method.
            if target_file is None:
                new_file = {"file": ("dummy", io.BytesIO())}
            elif target_file.lower().endswith(".csv.gz"):
                new_file = {"file": open(target_file, "rb")}
            elif target_file.lower().endswith(".csv"):
                upload_filename = os.path.basename(target_file)
                upload_filename += ".gz"

                with open(target_file, "rb") as in_file:
                    new_file = {"file": (upload_filename, gzip.compress(in_file.read()))}

            # Create the file container and get the ID.  We use the
            # file container ID to load the file and then return the
            # value to the caller for use in a data change call.

            if resolved_fid is None:
                # The caller is asking us to create a new container.
                file_container_response = self.fileContainers_create()

                if file_container_response is None:
                    logger.error("Unable to create fileContainer")
                    return None

                resolved_fid = file_container_response["id"]

            results['id'] = resolved_fid

            logger.debug(f"resolved fID: {resolved_fid}")

            # We have our container, load the file

            operation = f"/fileContainers/{resolved_fid}/files"
            logger.debug(f"fileContainer_load: POST {operation}")
            url = self.prism_endpoint + operation

            response = self.http_post(url, files=new_file)

            if response.status_code == 201:
                logger.debug(f"successfully loaded file: {file}")
                results['data'].append(response.json())

        results['total'] = len(results['data'])

        return results

    def wql_dataSources(self, id=None, alias=None, searchString=None, limit=None, offset=None):
        """Returns a collection of data sources for use in a WQL query.

        Parameters
        ----------
        id : str
            The ID of a Workday data source.
        alias : str
            Filters by alias match
        searchString : str
        """
        operation = '/dataSources'

        if id is not None:
            operation = f'{operation}/{id}'
            logger.debug(f'wql_dataSources: {operation}')
            url = f'{self.wql_endpoint}{operation}'

            response = self.http_get(url)

            return response.json()

        url_separator = '?'

        if alias is not None:
            operation += f'?alias={urlparse.quote(alias)}'
            url_separator = '&'
        elif searchString is not None:
            operation += f'?searchString={urlparse.quote(searchString)}'
            url_separator = '&'

        logger.debug(f'wql_dataSources: {operation}')
        url = f'{self.wql_endpoint}{operation}'

        # Always return a valid list - even if empty.
        return_sources = {'total': 0, 'data': []}

        if limit is not None and isinstance(limit, int) and 0 < limit <= 100:
            return_all = False

            query_limit = limit

            if offset is not None and isinstance(offset, int) and offset > 0:
                query_offset = offset
            else:
                query_offset = 0
        else:
            return_all = True

            query_limit = 100
            query_offset = 0

        # Assume we'll loop over more than one page.
        while True:
            r = self.http_get(f'{url}{url_separator}limit={query_limit}&offset={query_offset}')

            if r.status_code != 200:
                break

            ds = r.json()

            # Add this page to the final output.
            return_sources['data'] += ds['data']

            if not return_all:
                break

            if len(ds['data']) < query_limit:
                # A page size less than the limit means we are done.
                break

            query_offset += query_limit

        # Fix-up the final total of sources.
        return_sources["total"] = len(return_sources["data"])

        return return_sources

    def wql_dataSources_fields(self, id=None, alias=None, searchString=None, limit=None, offset=None):
        """Retrieves a field of the data source instance.
        
        Parameters
        ----------
        id : str
            The Workday ID of the resource.
        alias : str
            The alias of the data source field.
        searchString : str
            The string to be searched in case-insensitive manner within the descriptors of the data source fields.
        limit : int
            The maximum number of objects in a single response. The default is 20, the maximum is 100, and None is all.
        offset : int
            The zero-based index of the first object in a response collection.
        operation = '/dataSources'
        """

        if id is None:
            return None

        operation = f'/dataSources/{id}/fields'
        logger.debug('wql_dataSources_fields: {operation}')
        url = f'{self.wql_endpoint}{operation}'

        url_separator = '?'

        if alias is not None:
            operation += f'?alias={urlparse.quote(alias)}'
            url_separator = '&'

        if searchString is not None:
            operation += f'{url_separator}searchString={urlparse.quote(searchString)}'
            url_separator = '&'

        if limit is not None and isinstance(limit, int) and 0 < limit <= 100:
            return_all = False

            query_limit = limit

            if offset is not None and isinstance(offset, int) and offset > 0:
                query_offset = offset
            else:
                query_offset = 0
        else:
            return_all = True

            query_limit = 100
            query_offset = 0

        return_fields = {'total': 0, 'data': []}

        while True:
            url = f'{url}{url_separator}limit={query_limit}&offset={query_offset}'

            response = self.http_get(url)

            if response.status_code != 200:
                break

            fields = response.json()

            # Add this page of fields to the final output.
            return_fields['data'] += fields['data']

            if not return_all:
                break

            if len(fields['data']) < query_limit:
                # A page size less than the limit means we are done.
                break

            query_offset += query_limit

        return_fields['total'] = len(return_fields['data'])

        return return_fields

    def wql_data(self, query, limit, offset):
        """Returns the data from a WQL query.

        Parameters
        ----------
        query : str
            The WQL query that retrieves the data.
        limit: int
            The maximum number of objects in a single response - maximum 10,000.
        offset: int
            The zero-based index of the first object in a response collection.

        Returns
        -------
        dict
            Returns a dict with a "total" row count attribute and a "data"
            array of rows.
        """
        operation = '/data'

        url = f'{self.wql_endpoint}{operation}'
        query_safe = urlparse.quote(query.strip())

        if limit is None or not isinstance(limit, int) or limit > 10000:
            query_limit = 10000
            offset = 0
        else:
            query_limit = limit

        offset = offset if offset is not None and isinstance(offset, int) else 0

        # Always return a valid object - even if no rows are returned.
        data = {'total': 0, 'data': []}

        while True:
            r = self.http_get(f'{url}?query={query_safe}&limit={query_limit}&offset={offset}')

            if r.status_code == 200:
                page = r.json()
                data['data'] += page['data']
            else:
                # There was a problem, return whatever we have...
                return data

            if len(page['data']) < query_limit:
                break

            offset += query_limit

        # Set the final row count.
        data['total'] = len(data['data'])

        return data

    def raas_run(self, report, user, params=None, format_='XML'):
        """
        Run a Workday system or custom report.

        Parameters
        ----------
        report : str
            Name of the Workday report to run.
        user : str
            Username to include on URL
        params : list
            Array of parameter/value pairs to include on the URL
        format_ : str
            Output format, i.e., XML, JSON, CSV
        """
        if user is None or not isinstance(user, str) or len(user) == 0:
            logger.warning("generating delivered report (systemreport2).")
            url = f"{self.raas_endpoint}/systemreport2/{self.tenant_name}/{report}"
        else:
            logger.debug(f"generating report as {user}.")
            url = f"{self.raas_endpoint}/customreport2/{self.tenant_name}/{user}/{report}"

        separator = "?"
        if params is not None and len(params) > 0:
            query_str = ""

            for param in range(0, len(params), 2):
                query_str += separator + params[param] + "=" + params[param + 1]
                separator = "&"

            url += query_str

        if format_:
            url = f"{url}{separator}format={format_}"

        r = self.http_get(url)

        if r.status_code == 200:
            return r.text

        logging.error("HTTP Error: {}".format(r.content.decode("utf-8")))
        return None


def resolve_file_list(files):
    """Evaluate file name(s)s and return the list of supported files.

    Parameters
    ----------
    files : str|list
        One (str) or more (list) file names.

    Returns
    -------
    list
        List of files that can be uploaded.
    """
    # At a minimum, an empty list will always be returned.
    target_files = []

    if files is None:
        logger.warning("File(s) must be specified.")
        return target_files
    elif isinstance(files, list) and len(files) == 0:
        logger.warning("File(s) must be specified.")
        return target_files
    elif isinstance(files, tuple) and len(files) == 0:
        logger.warning("File(s) must be specified.")
        return target_files
    elif isinstance(files, str):
        if not files:
            logger.warning("File(s) must be specified.")
            return target_files
        else:
            files = [files]

    # Check the extension of each file in the list.
    for f in files:
        if not os.path.exists(f):
            logger.warning(f"File {f} not found - skipping.")
            continue

        if f.lower().endswith(".csv") or f.lower().endswith(".csv.gz"):
            target_files.append(f)
        else:
            logger.warning(f"File {f} is not a .csv.gz or .csv file - skipping.")

    return target_files
