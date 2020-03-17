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
formatter = logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s',
    "%Y-%m-%d %H:%M:%S")
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
        A dictionary containing the schema for your dataset.

    """
    with open(filename) as f:
        schema = json.load(f)

    return schema


class Prism:
    """Base class for interacting with the Workday Prism API.

    Attributes
    ----------
    base_url : str
        The base URL for the API client

    tenant_name : str
        The name of your Workday tenant

    client_id : str
        The Client ID for your registered API client

    client_secret : str
        The Client Secret for your registered API client

    refresh_token : str
        The Refresh Token for your registered API client

    """

    def __init__(self, base_url, tenant_name, client_id,
                 client_secret, refresh_token):
        """Init the Prism class with required attribues."""
        self.base_url = base_url
        self.tenant_name = tenant_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.token_endpoint = "{}/ccx/oauth2/{}/token".format(
            base_url, tenant_name)
        self.rest_endpoint = "{}/ccx/api/v1/{}".format(base_url, tenant_name)
        self.prism_endpoint = "{}/ccx/api/prismAnalytics/v1/{}".format(
            base_url, tenant_name
        )
        self.upload_endpoint = "{}/wday/opa/tenant/{}/service/wBuckets".format(
            base_url, tenant_name
        )
        self.bearer_token = None

    def create_bearer_token(self):
        """Exchange a refresh token for an access token.

        Parameters
        ----------
        None

        Returns
        -------
        If the request is succesful, the access token is added to the Prism()
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

        if r.status_code == 200:
            logging.info("Successfully obtained bearer token")
            logging.info("token_endpoint=" + self.token_endpoint)
            self.bearer_token = r.json()["access_token"]
        else:
            logging.warning("HTTP Error {}".format(r.status_code))

    def create_dataset(self, dataset_name):
        """Create an empty dataset of type "API".

        Parameters
        ----------
        dataset_name : str
            The dataset name. The name must be unique and conform to the name
            validation riles.

        Returns
        -------
        If the request is succesful, a dictionary containing information about
        the new dataset is returned.

        """
        url = self.prism_endpoint + "/datasets"

        headers = {
            "Authorization": "Bearer " + self.bearer_token,
            "Content-Type": "application/json",
        }

        data = {"name": dataset_name}

        r = requests.post(url, headers=headers, data=json.dumps(data))

        if r.status_code == 201:
            logging.info("Successfully created an empty API dataset")
            logging.info("url=" + url)
            return r.json()
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning("HTTP Error {}".format(r.status_code))

    def create_bucket(self, schema, dataset_id):
        """Create a temporary bucket to upload files.

        Parameters
        ----------
        schema : dict
            A dictionary containing the schema for your dataset.

        dataset_id : str
            The ID of the dataset that this bucket is to be associated with.

        Returns
        -------
        If the request is succesful, a dictionary containing information about
        the new bucket is returned.

        """
        url = self.prism_endpoint + "/wBuckets"

        headers = {
            "Authorization": "Bearer " + self.bearer_token,
            "Content-Type": "application/json",
        }

        data = {
            "name": "bucket_" + str(random.randint(100000, 999999)),
            "operation": {"id": "Operation_Type=Replace"},
            "targetDataset": {"id": dataset_id},
            "schema": schema,
        }

        r = requests.post(url, headers=headers, data=json.dumps(data))

        if r.status_code == 201:
            logging.info("Successfully created a new wBucket")
            logging.info("url=" + url)
            return r.json()
        elif r.status_code == 400:
            logging.warning(r.json()["errors"][0]["error"])
        else:
            logging.warning("HTTP Error {}".format(r.status_code))

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

        if r.status_code == 200:
            logging.info("Successfully uploaded file to the bucket")
            logging.info("url=" + url)
        else:
            logging.warning("HTTP Error {}".format(r.status_code))

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

        if r.status_code == 201:
            logging.info("Successfully completed the bucket")
            logging.info("url=" + url)
        else:
            logging.warning("HTTP Error {}".format(r.status_code))

    def list_bucket(self, bucket_id=None):
        """Obtain details for all buckets or a given bucket.

        Parameters
        ----------
        bucket_id : str
            The ID of the bucket to obtain datails about. If the default value
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

        if r.status_code == 200:
            logging.info(
                "Successfully obtained information about your buckets")
            logging.info("url=" + url)
            return r.json()
        else:
            logging.warning("HTTP Error {}".format(r.status_code))

    def list_dataset(self, dataset_id=None):
        """Obtain details for all datasets or a given dataset.

        Parameters
        ----------
        dataset_id : str
            The ID of the dataset to obtain datails about. If the default value
            of None is specified, details regarding all datasets is returned.

        Returns
        -------
        If the request is successful, a dictionary containing information about
        the dataset is returned.

        """
        url = self.prism_endpoint + "/datasets"

        if dataset_id is not None:
            url = url + "/" + dataset_id

        headers = {"Authorization": "Bearer " + self.bearer_token}

        r = requests.get(url, headers=headers)

        if r.status_code == 200:
            logging.info(
                "Successfully obtained information about your datasets")
            logging.info("url=" + url)
            return r.json()
        else:
            logging.warning("HTTP Error {}".format(r.status_code))
