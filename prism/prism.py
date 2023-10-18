"""
Load data into Workday Prism Analytics.

The Prism API provides a flexible, secure and scalable way to load data into
Workday Prism Analytics.
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
    # Resolve the log level - default to info if empty or invalid.
    if log_level is None:
        set_level = logging.INFO
    else:
        # Make sure the caller gave us a valid "name" for logging level.
        if hasattr(logging, log_level):
            set_level = getattr(logging, log_level)
        else:
            set_level = getattr(logging, "INFO")

    # If no file was specified, simply loop over any handlers and
    # set the logging level.
    if log_file is None:
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
    elapsed = timedelta.total_seconds()
    logging.getLogger(__name__).debug(f"{msg}: elapsed {elapsed:.5f}")


def buckets_gen_name():
    bucket_name = "cli_" + uuid.uuid4().hex
    logger.debug(f"buckets_gen_name: created bucket name: {bucket_name}")

    return bucket_name


def validate_schema(schema):
    if "fields" not in schema or not isinstance(schema["fields"], list) or len(schema["fields"]) == 0:
        logger.error("fields attribute missing from schema!")
        return False

    # Add a sequential order (ordinal) on the fields to (en)force
    # required sequencing of fields.
    for ordinal in range(len(schema["fields"])):
        schema["fields"][ordinal]["ordinal"] = ordinal + 1

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

    # Now trim our fields data to keep just what we need
    for fld in fields:
        del fld["id"]
        del fld["displayName"]
        del fld["fieldId"]
        del fld["required"]
        del fld["externalId"]

    # Build the final bucket definition.
    bucket_schema = {
        "parseOptions": {
            "fieldsDelimitedBy": ",",
            "fieldsEnclosedBy": '"',
            "headerLinesToIgnore": 1,
            "charset": {"id": "Encoding=UTF-8"},
            "type": {"id": "Schema_File_Type=Delimited"},
        },
        "schemaVersion": {"id": "Schema_Version=1.0"},
        "fields": fields
    }

    return bucket_schema


class Prism:
    """Base class for interacting with the Workday Prism API.

    Attributes
    ----------
    base_url : str
        The URL for the API client

    tenant_name : str
        The name of your Workday tenant

    client_id : str
        The Client ID for your registered API client

    client_secret : str
        The Client Secret for your registered API client

    refresh_token : str
        The Refresh Token for your registered API client

    version : str
        The version of the Prism API to use
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
        self.raas_endpoint = f"{base_url}/ccx/service"

        # At creation, there cannot yet be a bearer_token obtained from Workday.
        self.bearer_token = None
        self.bearer_token_timestamp = None

        self.CONTENT_APP_JSON = {"Content-Type": "application/json"}
        self.CONTENT_FORM = {"Content-Type": "application/x-www-form-urlencoded"}

    def http_get(self, url, headers=None, params=None):
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
            log_elapsed(f"get: {caller}", response.elapsed)

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

        # Call requests directly for this one get operation.
        r = self.http_post(url=self.token_endpoint, headers=self.CONTENT_FORM, data=data)

        if r.status_code == 200:
            logger.debug("successfully obtained bearer token")
            self.bearer_token = r.json()["access_token"]
            self.bearer_token_timestamp = time.time()
        else:
            logger.error(f"create bearer token failed: HTTP status code {r.status_code}: {r.content}")
            self.bearer_token = None
            self.bearer_token_timestamp = None

    def get_bearer_token(self):
        """
        Get the current bearer token, or create a new one if it doesn't exist, or it's older than 15 minutes.
        """
        if self.bearer_token is None or (time.time() - self.bearer_token_timestamp) > 900:
            self.create_bearer_token()

        if self.bearer_token is None:
            return ""

        return self.bearer_token

    def reset_bearer_token(self):
        """Remove the current bearer token to force getting a new token on the next API call."""
        self.bearer_token = None
        self.bearer_token_timestamp = None

    def tables_list(
            self,
            name=None, wid=None,
            limit=None, offset=None,
            type_="summary",
            search=False):
        """Obtain details for all tables or a given table(s).

        Parameters
        ----------
        name : str
            The name of the table to obtain details about. If the default value
            of None is specified, details regarding first 100 tables is returned.

        wid : str
            The ID of a table to obtain details about.  When specified, all tables
            are searched for the matching id.

        limit : int
            The maximum number of tables to be queried, to the maximum of 100.

        offset: int
            The offset from zero of tables to return.

        type_ : str
            Level of detail to return.

        search : bool
            Enable substring searching for table names or ids

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the table is returned.

        """
        operation = "/tables"
        logger.debug(f"get: {operation}")

        url = self.prism_endpoint + operation

        if type_ is None or type_ not in ["full", "summary", "permissions"]:
            logger.warning("Invalid type for tables list operation - defaulting to summary.")
            type_ = "summary"

        # Start setting up the API call parameters.
        params = {}

        # See if we want to add an explicit table name as a search parameter.
        if not search and name is not None:
            # Here, the caller is not searching, they gave us an exact name.
            params["name"] = name.replace(" ", "_")  # Minor clean-up

            # Should only be 0 (not found) or 1 (found) tables found.
            limit = 1
            offset = 0

        # When searching by name or id, set the maximum limit size to
        # reduce the number of individual REST API calls.
        if search:
            limit = 100
            offset = 0

        # If we didn't get a limit, set it to the maximum supported by the API
        if limit is None:
            search = True  # Force a search so we get all tables
            limit = 100  # The caller didn't say
            offset = 0  # Offset cannot be anything other than zero

        offset = offset if offset is not None else 0

        # Finalized the parameters to the GET:/tables call.
        params["limit"] = limit
        params["offset"] = offset
        params["type"] = type_

        # Always return a valid JSON object of results regardless of
        # errors or API responses.
        return_tables = {"total": 0, "data": []}

        # Always assume we will retrieve more than one page.
        while True:
            r = self.http_get(url, params=params)

            if r.status_code != 200:
                logger.error(f"Tables list - invalid HTTP return code: {r.status_code}")

                # Whatever we have captured (perhaps nothing) so far will
                # be returned due to unexpected status code.
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
                match_tables = [tab for tab in tables["data"] if name in tab["name"] or name in tab["displayName"]]
            elif wid is not None:
                # User is looking for a table by ID
                match_tables = [tab for tab in tables["data"] if wid == tab["id"]]
            else:
                # Grab all the tables in the result
                match_tables = tables["data"]

            return_tables["data"] += match_tables

            # If we get back anything but a full page, we are done
            # paging the results.
            if len(tables["data"]) < limit:
                break

            if search:
                # Move on to the next page.
                offset += limit
                params["offset"] = offset
            else:
                # The caller asked for a specific limit and offset, exit the loop.
                break

        # We always return a valid JSON.
        return_tables["total"] = len(return_tables["data"])  # Separate step for debugging.
        return return_tables

    def tables_create(self, schema):
        """Create an empty table of type "API".

        Parameters
        ----------
        schema : list
            A dictionary containing the schema

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the new table is returned.

        """
        operation = "/tables"
        logger.debug(f"POST : {operation}")
        url = self.prism_endpoint + "/tables"

        if not validate_schema(schema):
            logger.error("Invalid schema for create operation.")
            return None

        r = self.http_post(url=url, headers=self.CONTENT_APP_JSON, data=json.dumps(schema))

        if r.status_code == 201:
            return r.json()

        return None

    def tables_update(self, wid, schema, truncate=False):
        """
        Update the schema of an existing table.

        """

        operation = f"/tables/{wid}"
        logger.debug(f"PUT: {operation}")
        url = self.prism_endpoint + operation

        if not validate_schema(schema):
            logger.error("Invalid schema for update operation.")
            return None

        headers = {
            "Authorization": "Bearer " + self.get_bearer_token(),
            "Content-Type": "application/json",
        }

        r = requests.put(url=url, data=schema)

        if r.status_code == 200:
            return r.json()

        logger.error(f"Error updating table {wid} - {r.text}.")
        return None

    def tables_patch(self, wid, displayName=None, description=None, documentation=None, enableForAnalysis=None,
                     schema=None):
        x = self.tenant_name
        return None

    def buckets_list(self,
                     wid=None, bucket_name=None,
                     limit=None, offset=None,
                     type_="summary",
                     table_name=None, search=False):
        """

        :param wid:
        :param bucket_name:
        :param limit:
        :param offset:
        :param type_:
        :param table_name:
        :param search:
        :return:
        """

        operation = "/buckets"
        logger.debug(f"get: {operation}")
        url = self.prism_endpoint + operation

        # Start the return object - this routine NEVER fails
        # and always returns a valid JSON object.
        return_buckets = {"total": 0, "data": []}

        # If we are searching, then we have to get everything first
        # so don't add a name to the bucket query.

        params = {"limit": limit if limit is not None else 100,
                  "offset": offset if offset is not None else 0}

        if not search and bucket_name is not None:
            # List a specific bucket name overrides any other
            # combination of search/table/bucket name/wid.
            params["name"] = bucket_name

            params["limit"] = 1
            params["offset"] = 0
        else:
            # Any other combination of parameters requires a search
            # through all the buckets in the tenant.
            search = True

            params["limit"] = 100  # Max pagesize to retrieve in the fewest REST calls.
            params["offset"] = 0

        if type_ in ["summary", "full"]:
            params["type"] = type_
        else:
            params["type"] = "summary"

        while True:
            r = self.http_get(url, params=params)

            if r.status_code != 200:
                # We never fail, return whatever we got (if any).
                logger.error("error listing buckets.")
                return return_buckets

            buckets = r.json()

            if not search and bucket_name is not None:  # Explicit bucket name
                # We are not searching, and we have a specific bucket,
                # return whatever we got with this call.
                return buckets

            # If we are not searching, simply append this page of results to
            # the return object.

            if bucket_name is not None:
                # Substring search for matching table names
                match_buckets = [bck for bck in buckets["data"] if
                                 bucket_name in bck["name"] or bucket_name in bck["displayName"]]
            elif wid is not None:
                # User is looking for a bucket by ID
                match_buckets = [bck for bck in buckets["data"] if wid == bck["id"]]
            elif table_name is not None:
                # Caller is looking for any/all buckets by target table
                match_buckets = [bck for bck in buckets["data"] if table_name in bck["targetDataset"]["descriptor"]]
            else:
                # Grab all the tables in the result - select all buckets.
                match_buckets = buckets["data"]

            # Add to the results.
            return_buckets["data"] += match_buckets
            return_buckets["total"] = len(return_buckets["data"])

            # If we get back a list of buckets fewer than a full page, we are done
            # paging the results.
            if len(buckets["data"]) < params["limit"]:
                break

            if search:
                # Figure out what to search for on the next page.
                params["offset"] += limit
            else:
                # The caller asked for a specific limit and offset, exit the loop.
                break

        # We always return a valid JSON.
        return return_buckets

    def buckets_create(
            self,
            name=None,
            target_name=None,
            target_wid=None,
            schema=None,
            operation="TruncateAndInsert"):
        """Create a temporary bucket to upload files.

        Parameters
        ----------
        name : str
            Name of the bucket to create.

        schema : dict
            A dictionary containing the schema for your table.

        target_wid : str
            The ID of the table that this bucket is to be associated with.

        target_name : str
            The name of the table that this bucket is to be associated with.

        operation : str
           Required, defaults to "TruncateandInsert" operation
           Additional Operations - “Insert”, “Update”, “Upsert”, “Delete”
           When you use Update/Upsert/Delete operation you must specify which field to use
           as the matching key by setting the ‘useAsOperationKey’ attribute on that field as True.
           Only fields marked as ExternalID or WPA_RowID or WPA_LoadId on Table schema can be used
           as operation keys during loads into the table.

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the new bucket is returned.

        https://confluence.workday.com/display/PRISM/Public+API+V2+Endpoints+for+WBuckets
        """

        # If the caller didn't give us a name to use for the bucket,
        # create a default name.
        if name is None:
            bucket_name = buckets_gen_name()
        else:
            bucket_name = name

        # A target table must be identified by ID or name.
        if target_wid is None and target_name is None:
            logger.error("A table id or table name is required to create a bucket.")
            return None

        # The caller gave us a table wid, but didn't include a schema. Make a copy
        # of the target table's schema.  Note: WID takes precedence over name.
        # Use type_=full to get back the schema definition.

        if target_wid is not None:
            tables = self.tables_list(wid=target_wid, type_="full")
        else:
            tables = self.tables_list(name=target_name, type_="full")

        if tables["total"] == 0:
            logger.error(f"Table not found for bucket operation.")
            return None

        table_id = tables["data"][0]["id"]

        if schema is None:
            schema = table_to_bucket_schema(tables["data"][0])

        logger.debug(f"post: /buckets")
        url = self.prism_endpoint + "/buckets"

        data = {
            "name": bucket_name,
            "operation": {"id": "Operation_Type=" + operation},
            "targetDataset": {"id": table_id},
            "schema": schema,
        }

        r = self.http_post(url, headers=self.CONTENT_APP_JSON, data=json.dumps(data))

        if r.status_code == 201:
            logger.info("successfully created a new wBucket")
            return r.json()

        return None

    def buckets_complete(self, bucketid):
        operation = f"/buckets/{bucketid}/complete"
        logger.debug(f"post: {operation}")
        url = self.prism_endpoint + operation

        r = self.http_post(url)

        if r.status_code == 201:
            logger.info(f"successfully completed wBucket {bucketid}")
            return r.json()

        return None

    def buckets_upload(self, bucketid, file=None):
        """Upload a file to a given bucket.

        Parameters
        ----------
        bucketid : str
            The ID of the bucket that the file should be added to.

        file : str
            The path to your file to upload to the bucket. The file must be
            gzip compressed delimited and the file must conform to the file
            size limits.

        Returns
        -------
        None

        """
        operation = f"/buckets/{bucketid}/files"
        logger.debug("post: {operation}")
        url = self.prism_endpoint + operation

        results = []

        # Convert a single filename to a list, so we can loop.
        if isinstance(file, list):
            files = file
        else:
            files = [file]  # Convert to list...

        for f in files:
            # It is legal to upload an empty file - see the table truncate method.
            if f is None:
                new_file = {"file": ("dummy", io.BytesIO())}
            elif f.lower().endswith(".csv.gz"):
                new_file = {"file": open(f, "rb")}
            elif f.lower().endswith(".csv"):
                with open(f, "rb") as in_file:
                    new_file = {"file": (f + ".gz", gzip.compress(in_file.read()))}

            r = requests.post(url, files=new_file)

            if r.status_code == 201:
                logger.debug(f"successfully uploaded {f} to the bucket")

                if isinstance(file, str):
                    # If we got a single file, return the first result.
                    return r.json()
                else:
                    results.append(r.json())

        return results

    def dataChanges_list(self,
                         name=None,
                         wid=None,
                         activity_id=None,
                         limit=None, offset=None,
                         type_="summary",
                         search=False,
                         refresh=False):
        # We are doing a dataChanges GET operation.
        operation = "/dataChanges"

        # If an ID is provided, add it to the URL as part of the path.
        if wid is not None and isinstance(wid, str) and len(wid) > 0:
            operation += f"/{wid}"
            search_by_id = True
        else:
            search_by_id = False

        logger.debug(f"get: {operation}")

        # We know what kind of list (all or specific DCT) we want, add in the
        # ability to search by name and pages.
        if type_ and isinstance(type_, str):
            if type_ == "summary":
                operation += "?type=summary"
            elif type_ == "full":
                operation += "?type=full"
            else:
                logger.warning(f'/dataChanges: invalid verbosity {type_} - defaulting to summary.')
                operation += "?type=summary"
        else:
            logger.warning("/dataChanges: invalid verbosity - defaulting to summary.")
            operation += "?type=summary"

        logger.debug(f"dataChanges_activities_get: {operation}")

        # Start building the full URL for the call
        url = self.prism_endpoint + operation

        # Searching by ID is a special case that eliminates all other types
        # of search.  Ask for the datachange by id and return just this
        # result - even blank.

        if search_by_id:
            response = self.http_get(url=url)

            if response.status_code == 200:
                return response.json()
            else:
                return None

        # Get a list of tasks by page, with or without searching.

        search_limit = 500  # Assume all DCTs should be returned - max API limit
        search_offset = 0  # API default value

        if limit is not None and isinstance(limit, int) and limit > 0:
            search_limit = limit

        if offset is not None and isinstance(offset, int) and offset > 0:
            search_offset = offset

        searching = False

        if name is not None and isinstance(name, str) and len(name) > 0:
            if search is not None and isinstance(search, bool) and search:
                # Force a return of ALL data change tasks, so we can search the names.
                searching = True

                search_limit = 500
                search_offset = 0
            else:
                # With an explicit name, we should return at most 1 result.
                url += "&name=" + urlparse.quote(name)

                searching = False
                search_limit = 1
                search_offset = 0

        # Assume we will be looping based on limit and offset values; however, we may
        # execute only once.  NOTE: this routine NEVER fails, but may return zero
        # data change tasks.

        data_changes = {"total": 0, "data": []}

        while True:
            search_url = f"{url}&limit={search_limit}&offset={search_offset}"
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

    def dataChanges_activitietrs_get(self, data_change_id, activity_id):
        operation = f"/dataChanges/{data_change_id}/activities/{activity_id}"
        logger.debug(f"dataChanges_activities_get: {operation}")
        url = self.prism_endpoint + operation

        r = self.http_get(url)

        if r.status_code == 200:
            return r.json()

        return None

    def dataChanges_activities_post(self, data_change_id, fileContainerID=None):
        operation = f"/dataChanges/{data_change_id}/activities"
        logger.debug(f"post: {operation}")
        url = self.prism_endpoint + operation

        if fileContainerID is None:
            logger.debug("no file container ID")
            data = None
        else:
            logger.debug("with file container ID: {fileContainerID")

            # NOTE: the name is NOT correct based on the API definition
            data = json.dumps({"fileContainerWid": fileContainerID})

        r = self.http_post(url, headers=self.CONTENT_APP_JSON, data=data)

        if r.status_code == 201:
            activity_id = r.json()["id"]

            logger.debug(f"Successfully started data load task - id: {activity_id}")
            return activity_id

        return None

    def dataChanges_by_name(self, data_change_name):
        logger.debug(f"data_changes_by_name: {data_change_name}")

        data_changes_list = self.dataChanges_list(name=data_change_name)

        for data_change in data_changes_list:
            if data_change.get("displayName") == data_change_name:
                # We found the DCT by name, lookup all the details.
                data_change_id = data_change.get("id")
                logger.debug(f"found {data_change_name}: {data_change_id}")

                return self.dataChanges_by_id(data_change_id)

        logger.debug(f"{data_change_name} was not found!")

        return None

    def dataChanges_by_id(self, data_change_id):
        operation = f"/dataChanges/{data_change_id}"
        logger.debug(f"dataChanges_by_id: {operation}")

        url = self.prism_endpoint + f"/dataChanges/{data_change_id}"

        headers = {"Authorization": "Bearer " + self.get_bearer_token()}

        r = self.http_get(url, headers=headers)

        if r.status_code == 200:
            logger.debug(f"Found data change task: id = {data_change_id}")

            return json.loads(r.text)

        return None

    def dataChanges_is_valid(self, data_change_id):
        dct = self.dataChanges_validate(data_change_id)

        if dct is None:
            logger.error(f"data_change_id {data_change_id} not found!")
            return False

        if "error" in dct:
            logger.critical(f"data_change_id {data_change_id} is not valid!")
            return False

        return True

    def dataChanges_validate(self, data_change_id):
        operation = f"/dataChanges/{data_change_id}/validate"
        logger.debug(f"dataChanges_validate: get {operation}")
        url = self.prism_endpoint + operation

        r = self.http_get(url)

        if r.status_code == 200:
            return r.json()

        return None

    def fileContainers_create(self):
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

    def fileContainers_list(self, filecontainerid):
        """
        
        :param filecontainerid:
        :return:
        """
        operation = f"/fileContainers/{filecontainerid}/files"
        logger.debug(f"fileContainers_list: get {operation}")
        url = self.prism_endpoint + operation

        r = self.http_get(url)

        if r.status_code == 200:
            return r.json()

        if r.status_code == 404:
            logger.warning("verify: Self-Service: Prism File Container domain in the Prism Analytics functional area")

        return []  # Always return a list.

    def fileContainers_load(self, fileContainerID, file):
        """
        Load one or more files to a fileContainer.

        :param fileContainerID:
        :param file:
        :return:
        """

        fid = fileContainerID  # Create target fID from param

        # Convert a single filename to a list, so we can loop.
        if isinstance(file, list):
            files = file
        else:
            files = [file]

        for file in files:
            # Do a sanity check and make sure the fi exists and
            # has a gzip extension.

            if not os.path.isfile(file):
                logger.warning(f"skipping - file not found: {file}")
                continue  # Next file...

            # It is legal to upload an empty file - see the table truncate method.
            if file is None:
                new_file = {"file": ("dummy", io.BytesIO())}
            elif file.lower().endswith(".csv.gz"):
                new_file = {"file": open(file, "rb")}
            elif file.lower().endswith(".csv"):
                with open(file, "rb") as in_file:
                    new_file = {"file": (file + ".gz", gzip.compress(in_file.read()))}

            # Create the file container and get the ID.  We use the
            # file container ID to load the file and then return the
            # value to the caller for use in a data change call.

            if fid is None:
                # The caller is asking us to create a new container.
                file_container_response = self.fileContainers_create()

                if file_container_response is None:
                    logger.error("Unable to create fileContainer")
                    return None

                fid = file_container_response["id"]

            logger.debug(f"resolved fID: {fid}")

            # We have our container, load the file

            operation = f"/fileContainers/{fid}/files"
            logger.debug(f"fileContainer_load: POST {operation}")
            url = self.prism_endpoint + operation

            r = self.http_post(url, files=new_file)

            if r.status_code == 201:
                logger.info(f"successfully loaded file to container: {file}")

        return fid

    def wql_dataSources(self, wid=None, limit=100, offset=0, dataSources_name=None, search=False):
        operation = "/dataSources"

        url = f"{self.wql_endpoint}{operation}"

        offset = 0
        return_sources = {"total": 0, "data": []}

        while True:
            r = self.http_get(f"{url}?limit=100&offset={offset}")

            if r.status_code == 200:
                ds = r.json()
                return_sources["data"] += ds["data"]
            else:
                return None

            if len(ds["data"]) < 100:
                break

            offset += 100

        return_sources["total"] = len(return_sources["data"])

        return return_sources

    def wql_data(self, query, limit, offset):
        operation = "/data"

        url = f"{self.wql_endpoint}{operation}"
        query_safe = urlparse.quote(query.strip())

        if limit is None or not isinstance(limit, int) or limit > 10000:
            query_limit = 10000
            offset = 0
        else:
            query_limit = limit

        offset = offset if offset is not None and isinstance(offset, int) else 0

        data = {"total": 0, "data": []}

        while True:
            r = self.http_get(f"{url}?query={query_safe}&limit={query_limit}&offset={offset}")

            if r.status_code == 200:
                page = r.json()
                data["data"] += page["data"]
            else:
                return data  # Return whatever we have...

            if len(page["data"]) < query_limit:
                break

            offset += query_limit

        # Set the final row count.
        data["total"] = len(data["data"])

        return data

    def raas_run(self, report, user, params=None, format_=None):
        """
        Run a Workday system or custom report.
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
        else:
            logging.error("HTTP Error: {}".format(r.content.decode("utf-8")))

        return None
