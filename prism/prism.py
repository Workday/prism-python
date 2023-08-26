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
import urllib
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# writing to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
log_format = logging.Formatter('[%(asctime)s] [%(levelname)s] - %(message)s')
handler.setFormatter(log_format)
logger.addHandler(handler)


def log_elapsed(msg, timedelta):
    elapsed = timedelta.total_seconds()
    logger.debug(f"{msg}: elapsed {elapsed:.5f}")


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
        self.raas_endpoint = f"{base_url}/ccx/service/customreport2/{tenant_name}"

        # At creation, there cannot yet be a bearer_token obtained from Workday.
        self.bearer_token = None
        self.bearer_token_timestamp = None

    @staticmethod
    def set_log_level(log_level):
        logger.setLevel(getattr(logging, log_level))   # Convert the string to the proper log level
        logger.debug("set log level: {log_level}")

    def get(self, url, headers=None, params=None, log_tag="generic get"):
        if url is None:
            logger.warning("http_get: missing URL")
            return None

        # Every request requires an authorization header - make it true.
        auth_attr = "Authorization"

        if headers is None:
            headers = {}

        if auth_attr not in headers:
            headers[auth_attr] = "Bearer " + self.get_bearer_token()

        response = requests.get(url, params=params, headers=headers)
        log_elapsed("GET: " + log_tag, response.elapsed)

        if response.status_code != 200:
            logger.error(f"Invalid HTTP status: {response.status_code}")

        return response

    def validate_schema(self, schema):
        if "fields" not in schema or not isinstance(schema["fields"], list) or len(schema["fields"]) == 0:
            logger.error("Invalid schema detected!")
            return False

        # Add a sequential order (ordinal) on the fields to (en)force
        # proper numbering.
        for ordinal in range(len(schema["fields"])):
            schema["fields"][ordinal]["ordinal"] = ordinal + 1

        return True

    def create_bearer_token(self):
        """Exchange a refresh token for an access token.

        Parameters
        ----------

        Returns
        -------
        If the request is successful, the access token is added to the Prism()
        class.

        """

        logger.debug("create_bearer_token")

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        r = requests.post(self.token_endpoint, headers=headers, data=data)
        log_elapsed("create_bearer_token", r.elapsed)

        if r.status_code == 200:
            logger.debug("successfully obtained bearer token")
            self.bearer_token = r.json()["access_token"]
            self.bearer_token_timestamp = time.time()
        else:
            logger.warning(f"HTTP status code {r.status_code}: {r.content}")
            self.bearer_token = None

    def get_bearer_token(self):
        """Get the current bearer token, or create a new one if it doesn't exist, or it's older than 15 minutes."""
        if self.bearer_token is None:
            self.create_bearer_token()

        if time.time() - self.bearer_token_timestamp > 900:
            self.create_bearer_token()

        return self.bearer_token

    def reset_bearer_token(self):
        """Remove the current bearer token to force getting a new token on the next API call."""
        self.bearer_token = None

    def tables_list(
            self,
            name=None,
            id=None,
            limit=None,
            offset=None,
            type_="summary",
            search=False):
        """Obtain details for all tables or a given table(s).

        Parameters
        ----------
        name : str
            The name of the table to obtain details about. If the default value
            of None is specified, details regarding first 100 tables is returned.

        id : str
            The ID of a table to obtain details about.  When specified, all tables
            are searched for the matching id.

        limit : int
            The maximum number of tables to be queried, to the maximum of 100.

        offset: int
            The offset from zero of tables to return.

        type_ : str
            details

        search : bool
            Enable substring searching for table names or ids

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the table is returned.

        """
        operation = "/tables"
        url = self.prism_endpoint + operation

        if type_ is None or type_ not in ["full", "summary", "permissions"]:
            logger.warning("Invalid return type for tables list operation.")
            type_ = "summary"

        # If we are searching, then we have to get everything using
        # limits and offsets, i.e., paging of results.

        params = {}

        # See if we want to add table name as a search parameter.
        if not search and name is not None:
            # Here, the user is not searching, they gave us an exact name.
            params["name"] = name.replace(" ", "_")  # Minor clean-up

            limit = 1  # Should only be 0 (not found) or 1 (found) tables found.
            offset = 0

        # When searching by name or id, set the maximum limit size to
        # reduce the number of individual REST API calls.
        if search:
            limit = 100
            offset = 0

        # If we didn't get a limit, set it to the maximum supported by the API
        if limit is None:
            search = True  # Force a search so we get all tables
            limit = 100

        if offset is None:
            offset = 0

        # Always assume we will retrieve more than one page
        params["limit"] = limit
        params["offset"] = offset
        params["type"] = type_

        # Always return a valid JSON object of results!
        return_tables = {"total": 0, "data": []}

        while True:
            r = self.get(url, params=params)

            if r.status_code != 200:
                logger.error(f"Invalid HTTP return code: {r.status_code}")
                break

            tables = r.json()

            if not search and name is not None:  # Explicit table name
                # We are not searching and we have a specific table - return whatever we got.
                return tables

            # If we are not searching, simply append all the results to the return object.

            if name is not None:
                # Substring search for matching table names
                match_tables = [tab for tab in tables["data"] if name in tab["name"]]
            elif id is not None:
                # User is looking for a table by ID
                match_tables = [tab for tab in tables["data"] if id == tab["id"]]
            else:
                # Grab all the tables in the result
                match_tables = tables["data"]

            return_tables["data"] += match_tables

            # If we get back anything but a full page, we are done
            # paging the results.
            if len(tables["data"]) < limit:
                break

            if search:
                # Figure out what to search for on the next page.
                offset += limit
                params["offset"] = offset
            else:
                # The caller asked for a specific limit and offset, exit the loop.
                break

        # We always return a valid JSON.
        return_tables["total"] = len(return_tables["data"])
        return return_tables

    def tables_create(self, table_name, schema):
        """Create an empty table of type "API".

        Parameters
        ----------
        table_name : str
            The table name. The name must be unique and conform to the name
            validation rules.

        schema : list
            A list of dictionaries containing the schema

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the new table is returned.

        """
        operation = "/tables"
        logger.debug(f"POST : {operation}")

        url = self.prism_endpoint + "/tables"

        if not self.validate_schema(schema):
            return None

        headers = {
            "Authorization": "Bearer " + self.get_bearer_token(),
            "Content-Type": "application/json",
        }

        r = requests.post(url, headers=headers, data=json.dumps(schema))

        if r.status_code == 201:
            logging.info("Successfully created an empty API table")
            return r.json()
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

        return None

    def tables_update(self, name, schema):
        tables = self.tables(name=name)

        # We never fail - if the table doesn't exist, only
        # log a warning.

        if tables["total"] == 0:
            # Assume we are doing a create
            table = self.tables_create(name, schema)
            return None

    def tables_patch(self, id, displayName=None, description=None, documentation=None, enableForAnalysis=None, schema=None):
        return None

    def buckets_list(self,
                     wid=None,
                     bucket_name=None,
                     limit=None,
                     offset=None,
                     type_="summary",
                     table_name=None,
                     search=False):

        operation = "/buckets"
        url = self.prism_endpoint + operation

        # Start the return object - this routine NEVER fails
        # and always returns a valid JSON object.
        return_buckets = {"total": 0, "data": []}

        # If we are searching, then we have to get everything first
        # so don't add a name to the bucket.

        params = {}

        if not search and bucket_name is not None:
            # List a specific bucket name overrides any other
            # combination of search/table/bucket name/wid.
            params["name"] = bucket_name

            limit = 1
            offset = 0
        else:
            # Any other combination of parameters requires a search.
            search = True
            limit = 100  # Max pagesize to retrieve in the fewest REST calls.
            offset = 0

        if limit is not None:
            params["limit"] = limit
            params["offset"] = offset if offset is not None else 0
        else:
            params["limit"] = 100
            params["offset"] = 0

        if type_ in ["summary", "full"]:
            params["type"] = type_
        else:
            params["type"] = "summary"

        while True:
            r = self.get(url, params=params, log_tag=operation)

            if r.status_code != 200:
                return return_buckets

            buckets = r.json()

            if not search and bucket_name is not None:  # Explicit bucket name
                # We are not searching, and we have a specific bucket,
                # return whatever we got.
                return buckets

            # If we are not searching, simply append all the results to the return object.

            if bucket_name is not None:
                # Substring search for matching table names
                match_buckets = [bck for bck in buckets["data"] if bucket_name in bck["name"]]
            elif wid is not None:
                # User is looking for a bucket by ID
                match_buckets = [bck for bck in buckets["data"] if wid == bck["id"]]
            elif table_name is not None:
                # Caller is looking for any/all buckets by target table
                match_buckets = [bck for bck in buckets["data"] if table_name in bck["targetDataset"]["descriptor"]]
            else:
                # Grab all the tables in the result - select all buckets.
                match_buckets = buckets["data"]

            return_buckets["data"] += match_buckets
            return_buckets["total"] = len(return_buckets["data"])

            # If we get back anything but a full page, we are done
            # paging the results.
            if len(buckets["data"]) < params["limit"]:
                break

            if search:
                # Figure out what to search for on the next page.
                params["offset"] += params["limit"]
            else:
                # The caller asked for a specific limit and offset, exit the loop.
                break

        # We always return a valid JSON.
        return return_buckets

    def buckets_create(
            self,
            name,
            target_id=None,
            target_name=None,
            schema=None,
            operation="TruncateandInsert"):
        """Create a temporary bucket to upload files.

        Parameters
        ----------
        schema : dict
            A dictionary containing the schema for your table.

        target_id : str
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

        # A target table must be identified by ID or name.
        if target_id is None and target_name is None:
            logger.error("A table id or table name is required to create a bucket.")
            return None

        # The caller didn't include a schema, make a copy of the target table's schema.
        if target_id is not None and schema is None:
            tables = self.tables_list(table_id=target_id, type_="full")

            if tables["total"] == 0:
                logger.error(f"Table ID {target_id} does not exist for bucket operation.")
                return None

            schema = tables["data"][0]["fields"]

        if target_id is None:
            tables = self.tables_list(api_name=target_name, type_="full")

            if tables["total"] == 0:
                logger.error(f"Table {target_name} does not exist for create bucket operation.")
                return None

            target_id = tables["data"]["0"]["id"]

            if schema is None:
                schema = tables["data"]["0"]["fields"]

        url = self.prism_endpoint + "/buckets"

        headers = {
            "Authorization": "Bearer " + self.bearer_token,
            "Content-Type": "application/json",
        }

        data = {
            "name": name,
            "operation": {"id": "Operation_Type=" + operation},
            "targetDataset": {"id": target_id},
            "schema": schema,
        }

        r = requests.post(url, headers=headers, data=data)

        if r.status_code == 201:
            logging.info("Successfully created a new wBucket")
            return r.json()
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

        return None

    def buckets_complete(self, bucketid):
        url = self.prism_endpoint + f"/buckets/{bucketid}/complete"

        headers = {
            "Authorization": "Bearer " + self.bearer_token
        }

        r = requests.post(url, headers=headers)

        if r.status_code == 201:
            logging.info("Successfully created a new wBucket")
            return r.json()

        if r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

        return None

    def table_to_bucket_schema(self, table):
        """Convert schema (derived from describe table) to bucket schema

        Parameters
        ----------
        describe_schema: dict
            A dictionary containing the describe schema for your dataset.

        Returns
        -------
        If the request is successful, a dictionary containing the bucket schema is returned.
        The results can then be passed to the create_bucket function

        """

        # describe_schema is a python dict object and needs to be accessed as such, 'data' is the top level object,
        # but this is itself a list (with just one item) so needs the list index, in this case 0. 'fields' is found
        # in the dict that is in ['data'][0]

        if table is None or "fields" not in table:
            logger.critical("Invalid table passed to table_to_bucket_schema.")
            return None

        fields = table["fields"]

        # Create and assign useAsOperationKey field with true/false values based on externalId value
        operation_key_false = {"useAsOperationKey": False}
        operation_key_true = {"useAsOperationKey": True}

        for i in fields:
            if i["externalId"] is True:
                i.update(operation_key_true)
            else:
                i.update(operation_key_false)

        # Get rid of the WPA_ fields...
        fields[:] = [x for x in fields if "WPA" not in x["name"]]

        # Now trim our fields data to keep just what we need
        for i in fields:
            del i["id"]
            del i["displayName"]
            del i["fieldId"]
            del i["required"]
            del i["externalId"]

        # The "header" for the load schema
        bucket_schema = {
            "parseOptions": {
                "fieldsDelimitedBy": ",",
                "fieldsEnclosedBy": '"',
                "headerLinesToIgnore": 1,
                "charset": {"id": "Encoding=UTF-8"},
                "type": {"id": "Schema_File_Type=Delimited"},
            }
        }

        # The footer for the load schema
        schema_version = {"id": "Schema_Version=1.0"}

        bucket_schema["fields"] = fields
        bucket_schema["schemaVersion"] = schema_version

        return bucket_schema

    def buckets_upload(self, bucketid, filename):
        """Upload a file to a given bucket.

        Parameters
        ----------
        bucket_id : str
            The ID of the bucket that the file should be added to.

        filename : str
            The path to your file to upload to the bucket. The file must be
            gzip compressed delimited and the file must conform to the file
            size limits.

        Returns
        -------
        None

        """
        url = self.prism_endpoint + f"/buckets/{bucketid}/files"

        headers = {"Authorization": "Bearer " + self.get_bearer_token()
                   }

        files = {"file": open(filename, "rb")}

        r = requests.post(url, headers=headers, files=files)

        if r.status_code == 201:
            logging.info("Successfully uploaded file to the bucket")
            return r.json()
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")
            return None

    def dataChanges_list(self,
                         name=None,
                         wid=None,
                         activity_id=None,
                         limit=-1, offset=None,
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

        # We know what kind of list we want, add in the ability to
        # search by name and pages.
        if type_ and isinstance(type_, str):
            if type_ == "summary":
                operation += "?type=summary"
            elif type_ == "full":
                operation += "?type=full"
            else:
                operation += "?type=summary"
                logger.warning("/dataChanges: invalid verbosity {verbosity} - defaulting to summary.")

        logger.debug(f"dataChanges_activities_get: {operation}")

        # Start building the full URL for the call
        url = self.prism_endpoint + operation

        # Searching by ID is a special case that eliminates all other types
        # of search.  Ask for the datachange by id and return just this
        # result - even blank.

        if search_by_id:
            response = self.get(url=url, log_tag="dataChanges")

            if response.status_code == 200:
                return response.json()
            else:
                return None

        # Get a list of tasks by page, with or without searching.

        search_limit = 500  # Assume all DCTs should be returned
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
                # Should return at most 1 result.
                url += "&name=" + urllib.parse.quote(name)

                searching = False
                search_limit = 1
                search_offset = 0

        # Assume we will be looping based on limit and offset values; however, we may
        # execute only once.

        dataChanges = {"total": 0, "data": []}

        while True:
            search_url = f"{url}&limit={search_limit}&offset={search_offset}"
            logger.debug(f"dataChangesID url: {search_url}")

            response = self.get(url=search_url, log_tag=operation)

            if response.status_code != 200:
                break

            retJSON = response.json()

            if searching:
                # Only add matching rows
                dataChanges["data"] += \
                    filter(lambda dtc: dtc["name"].find(name) != -1 or
                                       dtc["displayName"].find(name) != -1,
                                         retJSON["data"])
            else:
                # Without searching, simply paste the current page to the list.
                dataChanges["data"] += retJSON["data"]
                break

            # If we didn't get a full page, then we done.
            if len(retJSON["data"]) < search_limit:
                break

            # Go to the next page.
            offset += search_limit

        dataChanges["total"] = len(dataChanges["data"])

        return dataChanges

    def dataChanges_activities_get(self, data_change_id, activity_id):
        operation = f"/dataChanges/{data_change_id}/activities/{activity_id}"
        logger.debug(f"dataChanges_activities_get: {operation}")

        r = self.get(self.prism_endpoint + operation)

        if r.status_code == 200:
            return json.loads(r.text)

        return None

    def dataChanges_activities_post(self, data_change_id, fileContainerID=None):
        operation = f"/dataChanges/{data_change_id}/activities"
        logger.debug(f"dataChanges_activities_post: {operation}")

        url = self.prism_endpoint + operation

        headers = {
            "Authorization": "Bearer " + self.bearer_token,
            "Content-Type": "application/json",
        }

        if fileContainerID is None:
            logger.debug("no file container ID")

            data = None
        else:
            logger.debug("with file container ID: {fileContainerID")

            data = json.dumps({"fileContainerWid": fileContainerID})

        r = requests.post(url, data=data, headers=headers)
        log_elapsed(f"POST {operation}", r.elapsed)

        if r.status_code == 201:
            activityID = json.loads(r.text)["id"]

            logging.debug(f"Successfully started data load task - id: {activityID}")
            return activityID
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

        return None

    def dataChanges_by_name(self, data_change_name):
        logger.debug(f"data_changes_by_name: {data_change_name}")

        data_changes_list = self.data_changes_list()

        for data_change in data_changes_list:
            if data_change.get("displayName") == data_change_name:
                # We found the DCT by name, lookup all the details.
                data_change_id = data_change.get("id")
                logger.debug(f"found {data_change_name}: {data_change_id}")

                return self.data_changes_by_id(data_change_id)

        logger.debug(f"{data_change_name} was not found!")

        return None

    def dataChanges_by_id(self, data_change_id):
        operation = f"/dataChanges/{data_change_id}"
        logger.debug(f"dataChanges_by_id: {operation}")

        url = self.prism_endpoint + f"/dataChanges/{data_change_id}"

        headers = {"Authorization": "Bearer " + self.get_bearer_token()}

        r = requests.get(url, headers=headers)
        log_elapsed(logger, operation, r.elapsed)
        r.raise_for_status()

        if r.status_code == 200:
            logger.debug(f"Found data change task: id = {data_change_id}")

            return json.loads(r.text)
        elif r.status_code == 400:
            logger.warning(r.json()["errors"][0]["error"])
        else:
            logger.warning(f"HTTP status code {r.status_code}: {r.content}")

        return json.loads(r.text)

    def dataChanges_is_valid(self, data_change_id):
        dtc = self.dataChanges_validate(data_change_id)

        if dtc is None:
            logger.critical(f"data_change_id {data_change_id} not found!")

            return False

        if "error" in dtc:
            logger.critical(f"data_change_id {data_change_id} is not valid!")

            return False

        return True

    def dataChanges_validate(self, data_change_id):
        operation = f"/dataChanges/{data_change_id}/validate"
        logger.debug(f"dataChanges_validate: GET {operation}")

        url = self.prism_endpoint + operation

        r = self.get(url)

        if r.status_code == 200:
            return json.loads(r.text)

        return None

    def fileContainers_create(self):
        operation = "/fileContainers"
        logger.debug(f"fileContainer_create: POST {operation}")

        url = self.prism_endpoint + operation

        headers = {"Authorization": "Bearer " + self.get_bearer_token()}

        r = requests.post(url, headers=headers)
        log_elapsed(f"POST {operation}", r.elapsed)

        if r.status_code == 201:
            return_json = r.json()

            fileContainerID = return_json["id"]
            logger.debug(f"successfully created file container: {fileContainerID}")

            return return_json
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

        return None

    def fileContainers_list(self, fileContainerID):
        operation = f"/fileContainers/{fileContainerID}/files"
        logger.debug(f"fileContainers_list: GET {operation}")

        url = self.prism_endpoint + operation

        r = self.get(url)

        if r.status_code == 200:
            return r.json()

        return None

    def fileContainers_load(self, fileContainerID, fqfn):
        # Do a sanity check and make sure the fqfn exists and
        # has a gzip extension.

        if not os.path.isfile(fqfn):
            logger.critical("file not found: {fqfn}")
            return None

        # Create the file container and get the ID.  We use the
        # file container ID to load the file and then return the
        # value to the caller for use in a data change call.

        if fileContainerID is None:
            file_container_response = self.fileContainers_create()

            if file_container_response is None:
                return None

            fID = file_container_response["id"]
        else:
            fID = fileContainerID

        print(self.fileContainers_list(fID))

        # We have our container, load the file

        headers = {
            "Authorization": "Bearer " + self.get_bearer_token()
        }

        operation = f"/fileContainers/{fID}/files"
        logger.debug(f"fileContainer_load: POST {operation}")

        files = {"file": open(fqfn, "rb")}

        url = self.prism_endpoint + operation

        r = requests.post(url, files=files, headers=headers)
        log_elapsed(f"POST {operation}", r.elapsed)

        if r.status_code == 201:
            logging.info("successfully loaded fileContainer")

            print(self.fileContainers_list(fID))

            return fID
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

        return None

    def wql_dataSources(self, wid=None, limit=100, offset=0, dataSources_name=None, search=False):
        operation = "/dataSources"

        url = f"{self.wql_endpoint}{operation}"

        offset = 0
        return_sources = {"total": 0, "data": []}

        while True:
            r = self.get(f"{url}?limit=100&offset={offset}")

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
        query_safe = urllib.parse.quote(query)

        offset = 0
        data = {"total": 0, "data": []}

        while True:
            r = self.get(f"{url}?query={query_safe}&limit=10000&offset={offset}")

            if r.status_code == 200:
                ds = r.json()
                data["data"] += ds["data"]
            else:
                return None

            if len(ds["data"]) < 10000:
                break

            offset += 100

        data["total"] = len(data["data"])

        return data

    def raas_run(self, report, user, format_):
        url = f"{self.raas_endpoint}/{user}/{report}?format={format_}"

        if url is None:
            raise ValueError("RaaS URL is required")
        else:
            if url.find("format=") == -1:
                output_format = "xml"
            else:
                output_format = url.split("format=")[1]

        headers = {"Accept": "text/csv"}
        r = self.get(url, headers=headers)

        if r.status_code == 200:
            # if output_format == "json":
            #     return r.json()["Report_Entry"]
            # elif output_format == "csv":
            #     return list(csv.reader(io.StringIO(r.content.decode("utf8"))))
            # else:
            #     raise ValueError(f"Output format type {output_format} is unknown")
            return r.text
        else:
            logging.warning("HTTP Error: {}".format(r.content.decode("utf-8")))
