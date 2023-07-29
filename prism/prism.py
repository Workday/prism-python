"""
Load data into Workday Prism Analytics.

The Prism API provides a flexible, secure and scalable way to load data into
Workday Prism Analytics.
"""

import logging
import json
import random
import requests

# set up basic logging
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def load_schema(filename):
    """Load schema from a JSON file.

    Parameters
    ----------
    filename : str
        The path to your file.

    Returns
    -------
    schema : dict
        A dictionary containing the schema for your table.

    """
    with open(filename) as f:
        schema = json.load(f)

    return schema


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

    def __init__(self, base_url, tenant_name, client_id, client_secret, refresh_token, version="v2"):
        """Init the Prism class with required attributes."""
        self.base_url = base_url
        self.tenant_name = tenant_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.token_endpoint = f"{base_url}/ccx/oauth2/{tenant_name}/token"
        self.version = version
        self.rest_endpoint = f"{base_url}/ccx/api/{version}/{tenant_name}"
        self.prism_endpoint = f"{base_url}/ccx/api/prismAnalytics/{version}/{tenant_name}"
        self.upload_endpoint = f"{base_url}/wday/opa/tenant/{tenant_name}/service/wBuckets"
        self.bearer_token = None

    def create_bearer_token(self):
        """Exchange a refresh token for an access token.

        Parameters
        ----------

        Returns
        -------
        If the request is successful, the access token is added to the Prism()
        class.

        """
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        r = requests.post(self.token_endpoint, headers=headers, data=data)
        r.raise_for_status()

        if r.status_code == 200:
            logging.info("Successfully obtained bearer token")
            self.bearer_token = r.json()["access_token"]
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

    def create_table(self, table_name, schema):
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
        url = self.prism_endpoint + "/datasets"

        headers = {
            "Authorization": "Bearer " + self.bearer_token,
            "Content-Type": "application/json",
        }

        data = {"name": table_name, "fields": schema}

        r = requests.post(url, headers=headers, data=json.dumps(data))
        r.raise_for_status()

        if r.status_code == 201:
            logging.info("Successfully created an empty API table")
            return r.json()
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

    def create_bucket(self, schema, table_id, operation="TruncateandInsert"):
        """Create a temporary bucket to upload files.

        Parameters
        ----------
        schema : dict
            A dictionary containing the schema for your table.

        table_id : str
            The ID of the table that this bucket is to be associated with.

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

        """
        url = self.prism_endpoint + "/wBuckets"

        headers = {
            "Authorization": "Bearer " + self.bearer_token,
            "Content-Type": "application/json",
        }

        data = {
            "name": "prism_python_wbucket_" + str(random.randint(1000000, 9999999)),
            "operation": {"id": "Operation_Type=" + operation},
            "targetDataset": {"id": table_id},
            "schema": schema,
        }

        r = requests.post(url, headers=headers, data=json.dumps(data))
        r.raise_for_status()

        if r.status_code == 201:
            logging.info("Successfully created a new wBucket")
            return r.json()
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

    def upload_file_to_bucket(self, bucket_id, filename):
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
        url = self.upload_endpoint + "/" + bucket_id + "/files"

        headers = {"Authorization": "Bearer " + self.bearer_token}

        files = {"file": open(filename, "rb")}

        r = requests.post(url, headers=headers, files=files)
        r.raise_for_status()

        if r.status_code == 200:
            logging.info("Successfully uploaded file to the bucket")
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

    def complete_bucket(self, bucket_id):
        """Finalize the bucket once all files have been added.

        Parameters
        ----------
        bucket_id : str
            The ID of the bucket to be marked as complete.

        Returns
        -------
        None

        """
        url = self.prism_endpoint + "/wBuckets/" + bucket_id + "/complete"

        headers = {
            "Authorization": "Bearer " + self.bearer_token,
            "Content-Type": "application/json",
        }

        data = {}

        r = requests.post(url, headers=headers, data=json.dumps(data))
        r.raise_for_status()

        if r.status_code == 201:
            logging.info("Successfully completed the bucket")
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

    def list_bucket(self, bucket_id=None):
        """Obtain details for all buckets or a given bucket.

        Parameters
        ----------
        bucket_id : str
            The ID of the bucket to obtain details about. If the default value
            of None is specified, details regarding all buckets is returned.

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the bucket is returned.

        """
        url = self.prism_endpoint + "/wBuckets"

        if bucket_id is not None:
            url = url + "/" + bucket_id

        headers = {"Authorization": "Bearer " + self.bearer_token}

        r = requests.get(url, headers=headers)
        r.raise_for_status()

        if r.status_code == 200:
            logging.info("Successfully obtained information about your buckets")
            return r.json()
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

    def list_table(self, table_name=None):
        """Obtain details for all tables or a given table.

        Parameters
        ----------
        table_name : str
            The name of the table to obtain details about. If the default value
            of None is specified, details regarding first 100 tables is returned.

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the table is returned.

        """
        url = self.prism_endpoint + "/datasets?"

        if table_name is not None:
            url = url + "name=" + table_name

        params = {"limit": 100}

        headers = {"Authorization": "Bearer " + self.bearer_token}

        r = requests.get(url, params=params, headers=headers)
        r.raise_for_status()

        if r.status_code == 200:
            logging.info("Successfully obtained information about your tables")
            return r.json()
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

    def describe_table(self, table_id=None):
        """Obtain details for for a given table

        Parameters
        ----------
        table_id : str
            The ID of the table to obtain details about. If the default value
            of None is specified, details regarding all tables is returned.

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the table is returned.

        """
        url = self.prism_endpoint + "/datasets/"

        if table_id is not None:
            url = url + table_id + "/describe"

        headers = {"Authorization": "Bearer " + self.bearer_token}

        r = requests.get(url, headers=headers)
        r.raise_for_status()

        if r.status_code == 200:
            logging.info("Successfully obtained information about your tables")
            return r.json()
        else:
            logging.warning(f"HTTP status code {r.status_code}: {r.content}")

    def convert_describe_schema_to_bucket_schema(self, describe_schema):
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
        fields = describe_schema["data"][0]["fields"]

        # Create and assign useAsOperationKey field with true/false values based on externalId value
        operation_key_false = {"useAsOperationKey": False}
        operation_key_true = {"useAsOperationKey": True}

        for i in fields:
            if i["externalId"] is True:
                i.update(operation_key_true)
            else:
                i.update(operation_key_false)

        # Now trim our fields data to keep just what we need
        for i in fields:
            del i["id"]
            del i["displayName"]
            del i["fieldId"]
            del i["required"]
            del i["externalId"]

        # Get rid of the WPA_ fields...
        fields[:] = [x for x in fields if "WPA" not in x["name"]]

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


def create_table(p, table_name, schema):
    """Create a new Prism table.

    Parameters
    ----------
    p : Prism
        Instantiated Prism class from prism.Prism()

    table_name : str
        The name of the table to obtain details about. If the default value
        of None is specified, details regarding first 100 tables is returned.

    schema : list
        A list of dictionaries containing the schema

    Returns
    -------
    If the request is successful, a dictionary containing information about
    the table is returned.
    """

    p.create_bearer_token()
    table = p.create_table(table_name, schema=schema)

    return table


def upload_file(p, filename, table_id, operation="TruncateandInsert"):
    """Create a new Prism table.

    Parameters
    ----------
    p : Prism
        Instantiated Prism class from prism.Prism()

    filename : str
        The path to you GZIP compressed file to upload.

    table_id : str
        The ID of the Prism table to upload your file to.

    operation : str (default = TruncateandInsert)
        The table load operation.
        Possible options include TruncateandInsert, Insert, Update, Upsert, Delete.

    Returns
    -------
    If the request is successful, a dictionary containing information about
    the table is returned.
    """

    p.create_bearer_token()
    details = p.describe_table(table_id)
    bucket_schema = p.convert_describe_schema_to_bucket_schema(details)
    bucket = p.create_bucket(bucket_schema, table_id, operation=operation)
    p.upload_file_to_bucket(bucket["id"], filename)
    p.complete_bucket(bucket["id"])
