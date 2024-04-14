"""
FastOpenData Client
===================

The Python client for FastOpenData.

To use this client to retrieve data, you must have an API key. You can use the
client itself to get a free API key that's suitable for evaluation purposes:

>>> from fastopendata_client import FastOpenData
>>> api_key = FastOpenData.get_free_api_key('YOUR_EMAIL_ADDRESS')

Save your API key somewhere; you will use it each time you invoke the FastOpenData
client. If you lose your key, you can call this method again with the same
email address.

The free API key is rate limited and not suitable for production purposes. To
subscribe to the FastOpenData service and receive a key that will provide unlimited
access, please visit https://fastopendata.com. If you have questions about the
service or your particular use-case, email zac@fastopendata.com.

The main class for this client is `FastOpenData` which is invoked like so:

>>> from fastopendata_client import FastOpenData
>>> session = FastOpenData(api_key="<YOUR_API_KEY>")
>>> data = session.request(free_form_query="123 Main Street, Tallahassee, FL, 12345")

`data` now contains a dictionary with all the data from the FastOpenData server.

If you have a Pandas dataframe, you can append new columns containing data from
FastOpenData by calling `FastOpenData.append_to_dataframe` and specifying which
columns contain address information. For example, if `COLUMN_NAME` contains
unstructured address strings, you can do this:

>>> import pandas as pd
>>> from fastopendata_client import FastOpenData
>>> session = FastOpenData(api_key="<YOUR_API_KEY>")
>>> df = pd.DataFrame(...)
>>> session.append_to_dataframe(df, free_form_query=COLUMN_NAME)

Now `df` contains many new columns containing data from the FastOpenData server.

If your dataframe has columns containing structured address information, you
can do:

>>> session.append_to_dataframe(
        df,
        address1=ADDRESS1_COLUMN,
        address2=ADDRESS2_COLUMN,
        city=CITY_COLUMN,
        state=STATE_COLUMN,
        zip_code=ZIP_CODE_COLUMN
    )

Note that you have the option of specifying either `free_form_query` or the column
names for structured address data, but not both. Doing so will raise an exception.
"""

import logging
import os
import pathlib
import sys
from csv import DictReader, DictWriter
from typing import Dict, List, Optional

import pandas as pd
import requests
from rich.progress import Progress
import toml

logging.basicConfig(level=logging.INFO)

SCRIPT_PATH = list(pathlib.Path(__file__).parts[:-1]) + ["config.toml"]
SCRIPT_PATH = pathlib.Path(*SCRIPT_PATH)
CONFIG = toml.load(SCRIPT_PATH)
BATCH_SIZE = CONFIG["client"]["batch_size"]
IP_ADDRESS = CONFIG["server"]["ip_address"]
PORT = CONFIG["server"]["port"]
SCHEME = CONFIG["server"]["scheme"]


class FastOpenDataSecurityException(Exception):
    """
    Raise if there is a problem with authentication such as
    a missing or invalid API key.
    """

    pass


class FastOpenDataClientException(Exception):
    """
    Raise for problems caused when invoking the FastOpenData
    client.
    """

    pass


class FastOpenDataConnectionException(Exception):
    """
    Raise for problems caused when connecting to the FastOpenData
    server.
    """

    pass


class FastOpenDataRateLimitException(Exception):
    """
    Raise for problems caused when the FastOpenData server
    rate limits the client.
    """

    pass


class FastOpenData:
    """
    The client for interacting with the FastOpenData service.
    """

    def __init__(
        self,
        ip_address: str = IP_ADDRESS,
        port: int = PORT,
        scheme: str = SCHEME,
        api_key: str = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("FASTOPENDATA_API_KEY", None)
        self.ip_address = ip_address
        self.port = str(port)
        self.scheme = scheme
        if not self.api_key:
            raise FastOpenDataSecurityException(
                "You must specify an API key when instantiating the `FastOpenData` class "
                "by passing `api_key=YOUR_API_KEY` or by setting the `FASTOPENDATA_API_KEY` "
                "environment variable."
            )
        self.url = f"{self.scheme}://{self.ip_address}:{self.port}"
        self.get_single_address_url = f"{self.url}/get_single_address"
        self.get_batch_address_url = f"{self.url}/batch"
        self.request_headers = {
            "Content-type": "application/json",
            "x-api-key": self.api_key,
        }

    @staticmethod
    def flatten_response_dict(response_dict: dict) -> dict:
        """
        Flatten the keys for the `response_dict` so that
        the values can be appended to the DataFrame.
        """
        flat_response_dict = {}
        for geography, subdict in response_dict.items():
            if not subdict:
                continue
            for attribute, value in subdict.items():
                column_name = ".".join([geography, attribute])
                flat_response_dict[column_name] = value
        return flat_response_dict

    @staticmethod
    def flatten_response_list(response_list: List[dict]) -> List[dict]:
        """
        Flatten a list of response dictionaries.
        """
        return [
            FastOpenData.flatten_response_dict(response_dict)
            for response_dict in response_list
        ]

    @classmethod
    def get_free_api_key(cls, email_address: str) -> str:
        """
        Get a free API key from FastOpenData for evaluation purposes.

        Args:
            email_address: Your email address.

        Returns:
            A free API key.
        """
        free_api_key_url = f"{SCHEME}://{IP_ADDRESS}:{PORT}/get_free_api_key"
        headers = {
            "Content-type": "application/json",
        }
        try:
            response = requests.get(
                free_api_key_url,
                params={"email_address": email_address},
                headers=headers,
            )
        except Exception as e:
            raise FastOpenDataConnectionException(
                f"Problem connecting to the FastOpenData server. {e}"
            )
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise e
        response_dict = response.json()
        return response_dict

    @staticmethod
    def check_request_paremeters(
        free_form_query: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        address1: Optional[str] = None,
        address2: Optional[str] = None,
        zip_code: Optional[str] = None,
    ) -> bool:
        '''
        Check that the request parameters are valid.

        Args:
            free_form_query: The free-form query to use for matching.
            city: The city.
            state: The state.
            address1: The address line 1.
            address2: The address line 2.
            zip_code: The zip code.

        Returns:
            True if the request parameters are valid.
        
        Raises:
            FastOpenDataClientException: If the request parameters are invalid.
        '''
        if not (free_form_query or city or state or address1 or zip_code):
            raise FastOpenDataClientException(
                "Must include either `free_form_query` or some combination of "
                "`city`, `state`, `address` and `zip_code` when making a request."
            )
        if free_form_query and (city or state or address1 or address2 or zip_code):
            raise FastOpenDataClientException(
                "Request included both `free_form_query` and `city`, `state`, "
                "`address`, or `zip_code`, which is not permitted. Choose either "
                "`free_form_query` or the various address fields."
            )
        return True

    @property
    def api_spec(self) -> dict:
        """
        Get the API specification from the FastOpenData server. We do this because
        the API specification is subject to change and we want to make sure that
        the client is always in sync with the server.
        """
        api_spec_url = f"{self.url}/openapi.json"
        response = requests.get(api_spec_url)
        response.raise_for_status()
        return response.json()

    @property
    def geography_columns_dict(self) -> dict:
        """
        Get the geography columns from the API specification.
        """
        api_spec = self.api_spec
        geography_columns_dict = {}
        for key, config in api_spec["components"]["schemas"]["FastOpenDataResponse"][
            "properties"
        ].items():
            key_path = config["allOf"][0]["$ref"].split("/")[1:]
            c = api_spec
            for subkey in key_path:
                c = c[subkey]
            properties = sorted(list(c["properties"].keys()))
            geography_columns_dict[key] = properties
        return geography_columns_dict

    @property
    def geography_columns_list(self) -> List[str]:
        """
        Get the geography columns from the API specification as a list.
        """
        column_list = []
        for key, value in self.geography_columns_dict.items():
            column_list += [f"{key}.{subkey}" for subkey in value]
        return column_list

    def request(
        self,
        free_form_query: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        address1: Optional[str] = None,
        address2: Optional[str] = None,
        zip_code: Optional[str] = None,
    ) -> dict:
        """
        Make a request for a single address from the FastOpenData service.

        Args:
            free_form_query: The free-form query to use for matching.
            city: The city.
            state: The state.
            address1: The address line 1.
            address2: The address line 2.
            zip_code: The zip code.

        Returns:
            A dictionary containing the response data.

        Raises:
            FastOpenDataClientException: If the request fails.
        """
        FastOpenData.check_request_paremeters(
            free_form_query=free_form_query,
            city=city,
            state=state,
            address1=address1,
            address2=address2,
            zip_code=zip_code,
        )
        response = requests.get(
            self.get_single_address_url,
            params={
                "free_form_query": free_form_query,
            },
            headers=self.request_headers,
        )
        FastOpenData.check_response(response)
        return response.json()

    @classmethod
    def check_response(cls, response: requests.Response) -> dict:
        """
        Check that the response from the FastOpenData server is valid.
        """
        try:
            response.json()
        except Exception as _:
            return {
                "success": False,
                "detail": "ServerError",
            }

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            # This works differently from the other exceptions because we use
            # the fastapi-limit library to rate limit the client.
            if e.response.status_code == 429:
                return {
                    "success": False,
                    "detail": "RateLimitException",
                }
            raise e

        def _test_single_response(response_dict: Dict) -> bool:
            if response_dict.get("detail", None) == "GeographyException":
                return {
                    "success": False,
                    "detail": "GeographyException",
                }
            elif response_dict.get("detail", None) == "AuthorizationException":
                return {
                    "success": False,
                    "detail": "AuthorizationException",
                }
            elif response_dict.get("detail", None) == "IncompleteDataException":
                return {
                    "success": False,
                    "detail": "IncompleteDataException",
                }
            elif response_dict.get("detail", None) == "NominatimQueryException":
                return {
                    "success": False,
                    "detail": "NominatimQueryException",
                }
            return True

        json_output = response.json()
        if isinstance(json_output, list):
            for response_dict in json_output:
                response_test = _test_single_response(response_dict)
                if not response_test:
                    return response_test
            else:
                pass
        return True

    def append_to_dataframe(
        self,
        df: pd.DataFrame,
        free_form_query_column: Optional[str] = "free_form_query",
        address1_column: Optional[str] = "address1",
        address2_column: Optional[str] = "address2",
        city_column: Optional[str] = "city",
        state_column: Optional[str] = "state",
        zip_code_column: Optional[str] = "zip_code",
        batch_size: Optional[int] = BATCH_SIZE,
    ) -> pd.DataFrame:
        """
        Convert the DataFrame to a list of dictionaries and send
        the list to the batch endpoint. Append the resulting data
        to the DataFrame.

        Args:
            df: A Pandas DataFrame.
            free_form_query_column: The free-form query to use for matching.
            address1_column: The address line 1.
            address2_column: The address line 2.
            city_column: The city.
            state_column: The state.
            zip_code_column: The zip code.

        Returns:
            None.

        Raises:
            FastOpenDataClientException: If the `free_form_query` and
                `structured_query` arguments are both specified, or neither
                is specified, or if the DataFrame is empty.
        """
        df_dict = df.to_dict(orient="records")
        batch_response = self.send_batch(
            df_dict,
            free_form_query_column=free_form_query_column,
            address1_column=address1_column,
            address2_column=address2_column,
            city_column=city_column,
            state_column=state_column,
            zip_code_column=zip_code_column,
            batch_size=batch_size,
        )
        batch_response = FastOpenData.flatten_response_list(batch_response)
        df_response = pd.DataFrame(batch_response)
        df_combined = pd.concat([df, df_response], axis=1)
        return df_combined

    def send_batch(
        self,
        batch: List[Dict],
        free_form_query_column: Optional[str] = None,
        address1_column: Optional[str] = None,
        address2_column: Optional[str] = None,
        city_column: Optional[str] = None,
        state_column: Optional[str] = None,
        zip_code_column: Optional[str] = None,
        batch_size: Optional[int] = BATCH_SIZE,
        progress_bar: Optional[bool] = True,
    ) -> List[Dict]:
        """
        Rename keys to match `free_form_query`, etc. Then send the
        list of dictionaries to the batch endpoint.
        """
        total_response: List[dict] = []
        total_batch_size = len(batch)
        if progress_bar:
            pbar = Progress(transient=True, expand=False)
            task = pbar.add_task("Sending batch", total=total_batch_size)
        while batch:
            sub_batch = batch[:batch_size]
            batch = batch[batch_size:]

            response = requests.post(
                self.get_batch_address_url,
                json={
                    "batch": sub_batch,
                },
                params={
                    "free_form_query_column": free_form_query_column,
                    "address1_column": address1_column,
                    "address2_column": address2_column,
                    "city_column": city_column,
                    "state_column": state_column,
                    "zip_code_column": zip_code_column,
                },
                headers=self.request_headers,
            )
            FastOpenData.check_response(response)
            total_response += response.json()
            if progress_bar:
                pbar.advance(task, len(sub_batch))
        return total_response

    def append_to_csv(
        self,
        input_csv: str = "",
        output_csv: str = "",
        free_form_query_column: Optional[str] = None,
        address1_column: Optional[str] = None,
        address2_column: Optional[str] = None,
        city_column: Optional[str] = None,
        state_column: Optional[str] = None,
        zip_code_column: Optional[str] = None,
        batch_size: Optional[int] = BATCH_SIZE,
    ):
        """
        Append data from FastOpenData to an existing CSV file.
        """
        if not free_form_query_column:
            print("Need to specify column containing address information")
            sys.exit(1)
        if not input_csv:
            print("You must provide the path of a CSV file.")
            sys.exit(1)
        if not output_csv:
            print("You must provide the path for the output CSV.")
            sys.exit(1)
        if not os.path.isfile(input_csv):
            print(f"CSV file {input_csv} does not exist.")
            sys.exit(1)
        if os.path.isfile(output_csv):
            print(f"Output file {output_csv} already exists.")
            sys.exit(1)

        counter = 0

        def _write_batch(_writer, _batch: List[dict]) -> None:
            '''
            Private method to write a batch of rows to the CSV file.
            '''
            batch_response = self.send_batch(
                _batch,
                free_form_query_column=free_form_query_column,
                address1_column=address1_column,
                address2_column=address2_column,
                city_column=city_column,
                state_column=state_column,
                zip_code_column=zip_code_column,
                batch_size=batch_size,
                progress_bar=False,
            )
            batch_response = FastOpenData.flatten_response_list(batch_response)
            for index, response in enumerate(batch_response):
                response.update(_batch[index])
                _writer.writerow(response)
        csv_file = open(input_csv, "r")
        for index, _ in enumerate(csv_file):
            pass
        total_batch_size = index
        csv_file.close()
        with open(output_csv, "w") as o:
            with open(input_csv, "r") as f:
                with Progress(transient=True, expand=False) as pbar:
                    task = pbar.add_task("Appending", total=total_batch_size)
                    reader = DictReader(f)
                    input_csv_column_list = reader.fieldnames
                    writer = DictWriter(
                        o, fieldnames=self.geography_columns_list + input_csv_column_list
                    )
                    writer.writeheader()
                    batch = []
                    for row in reader:
                        counter += 1
                        pbar.advance(task, 1)
                        batch.append(row)
                        if len(batch) == 10:
                            _write_batch(writer, batch)
                            batch = []
            if batch:
                _write_batch(writer, batch)


def main():
    """
    Just for testing.
    """
    pass


if __name__ == "__main__":
    main()
