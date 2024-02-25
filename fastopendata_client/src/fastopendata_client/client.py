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
import pprint
import sys
from csv import DictReader, DictWriter
from typing import Dict, List, Optional

import pandas as pd
import random_address
import requests
import toml
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)

SCRIPT_PATH = list(pathlib.Path(__file__).parts[:-1]) + ['config.toml']
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
    '''
    Raise for problems caused when connecting to the FastOpenData
    server.
    '''

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
                "by passing `api_key=YOUR_API_KEY`."
            )
        self.url = f"{self.scheme}://{self.ip_address}:{self.port}"
        self.get_single_address_url = f"{self.url}/get_single_address"

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
                free_api_key_url, params={"email_address": email_address}, headers=headers
            )
        except Exception as e:
            raise FastOpenDataConnectionException(
                "Problem connecting to the FastOpenData server."
            )
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise e
        response_dict = response.json()
        return response_dict

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
        Make a request from the FastOpenData service.

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
        headers = {
            "Content-type": "application/json",
        }
        response = requests.get(
            self.get_single_address_url,
            params={
                "free_form_query": free_form_query,
                "api_key": self.api_key
            },
            headers=headers,
        )
        try:
            response.json()
        except Exception as e:
            return {
                'success': False,
                'detail': 'ServerError',
            }
        if response.json().get('detail', None) == 'GeographyException':
            return {
                'success': False,
                'detail': 'GeographyException',
            }
        elif response.json().get('detail', None) == 'IncompleteDataException':
            return {
                'success': False,
                'detail': 'IncompleteDataException',
            }
        elif response.json().get('detail', None) == 'NominatimQueryException':
            return {
                'success': False,
                'detail': 'NominatimQueryException',
            }
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise e

        return response.json()

    def append_to_dataframe(
        self,
        df: pd.DataFrame,
        free_form_query: Optional[str] = "free_form_query",
        address1: Optional[str] = "address1",
        address2: Optional[str] = "address2",
        city: Optional[str] = "city",
        state: Optional[str] = "state",
        zip_code: Optional[str] = "zip_code",
        progressbar: Optional[bool] = True,
    ) -> None:
        """
        Call FastOpenData for each row in the DataFrame. Append
        new columns to the DataFrame containing the resulting
        data. We'll want to have a batch endpoint eventually.

        Args:
            df: A Pandas DataFrame.
            free_form_query: The free-form query to use for matching.
            address1: The address line 1.
            address2: The address line 2.
            city: The city.
            state: The state.
            zip_code: The zip code.

        Returns:
            None.

        Raises:
            FastOpenDataClientException: If the `free_form_query` and
                `structured_query` arguments are both specified, or neither
                is specified, or if the DataFrame is empty.
        """
        column_list = list(df.columns)
        match_mode = None
        if free_form_query in column_list:
            match_mode = "FREE_FORM_QUERY"
        elif all(
            column_name in column_list
            for column_name in [
                address1,
                city,
                state,
                zip_code,
            ]
        ):
            match_mode = "STRUCTURED_QUERY"
        else:
            pass
        if not match_mode:
            raise FastOpenDataClientException(
                "Must specify match mode for data append."
            )
        elif match_mode == "STRUCTURED_QUERY":
            raise FastOpenDataClientException("Haven't gotten to this yet.")
        if df.empty:
            raise FastOpenDataClientException(
                "Passed an empty DataFrame to append function."
            )
        row_counter = 0

        # We waste a few nanoseconds here, but it's easier for
        # my addled mind to track with this private function.
        def _flatten_response(response_dict: dict) -> dict:
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

        if progressbar:
            pbar = tqdm(total=df.shape[0])
        for index, row in df.iterrows():
            if progressbar:
                pbar.update(1)
            if match_mode == "FREE_FORM_QUERY":
                request_params = {
                    "free_form_query": row[free_form_query],
                }
            elif match_mode == "STRUCTURED_QUERY":
                request_params = {
                    "address1": row[address1],
                    "address2": row[address2],
                    "city": row[city],
                    "state": row[state],
                    "zip_code": row[zip_code],
                }
            else:
                raise Exception("This should never happen.")
            response_dict = self.request(**request_params)
            if response_dict is None:
                continue
            flat_response = _flatten_response(response_dict)
            if row_counter == 0:
                # First row is special because we need to get all
                # the column names.
                data_column_list = [column_name for column_name in flat_response.keys()]
                # Finally add the columns with `np.NaT` values everywhere
                df_to_concat = pd.DataFrame(
                    {
                        column_name: [pd.NaT for _ in range(df.shape[1])]
                        for column_name in data_column_list
                    }
                )
                pd.concat([df, df_to_concat], axis=1)
            # Now we can continue with the rest of the rows.
            row_counter += 1
            for column_name, value in flat_response.items():
                df.loc[index, column_name] = value

    def send_batch(
        self,
        batch: List[Dict],
        free_form_query: Optional[str] = None,
        address1: Optional[str] = None,
        address2: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip_code: Optional[str] = None,
    ) -> List[Dict]:
        """
        Rename keys to match `free_form_query`, etc. Then send the
        list of dictionaries to the batch endpoint.
        """
        for address in batch:
            data = self.request(
                free_form_query=address.get(free_form_query, None),
                address1=address.get(address1, None),
                address2=address.get(address2, None),
                city=address.get(city, None),
                state=address.get(state, None),
                zip_code=address.get(zip_code, None),
            )
            data = {"_fod_data_response": data}
            address.update(data)  # in-place updating (check this)
        return batch

    def append_to_csv(
        self,
        input_csv: str = "",
        output_csv: str = "",
        free_form_query: Optional[str] = None,
        address1: Optional[str] = None,
        address2: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        zip_code: Optional[str] = None,
    ):
        """
        Append data from FastOpenData to an existing CSV file.
        """
        if not free_form_query:
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
        geography_keys = [
            "cbsa_2013",
            "census_block_group_2019",
            "congressional_district",
            "county",
            "puma",
            "school_district",
            "state",
            "tract",
        ]

        with open(input_csv, "r") as f:
            reader = DictReader(f)
            batch = []
            response_list = []
            for row in reader:
                counter += 1
                batch.append(row)
                if len(batch) % BATCH_SIZE == 0:
                    batch_response = self.send_batch(
                        batch,
                        free_form_query=free_form_query,
                        address1=address1,
                        address2=address2,
                        city=city,
                        state=state,
                        zip_code=zip_code,
                    )
                    for response in batch_response:
                        for geography_key in geography_keys:
                            for data_point_name, data_point_value in response[
                                "_fod_data_response"
                            ][geography_key].items():
                                flattened_key = f"{geography_key}.{data_point_name}"
                                response[flattened_key] = data_point_value
                        del response["_fod_data_response"]
                    for batch_item, response_item in zip(batch, batch_response):
                        batch_item.update(response_item)
                    response_list += batch
                    batch = []
            if batch:
                batch_response = self.send_batch(
                    batch,
                    free_form_query=free_form_query,
                    address1=address1,
                    address2=address2,
                    city=city,
                    state=state,
                    zip_code=zip_code,
                )
                for response in batch_response:
                    for geography_key in geography_keys:
                        for data_point_name, data_point_value in response[
                            "_fod_data_response"
                        ][geography_key].items():
                            flattened_key = f"{geography_key}.{data_point_name}"
                            response[flattened_key] = data_point_value
                    del response["_fod_data_response"]
                for batch_item, response_item in zip(batch, batch_response):
                    batch_item.update(response_item)
                response_list += batch
                batch = []

        # TODO: Change this to stream rows one by one
        with open(output_csv, "w") as o:
            fieldnames = list(response_list[0].keys())
            output_csv = DictWriter(o, fieldnames=fieldnames)
            output_csv.writeheader()
            for row in response_list:
                output_csv.writerow(row)


def main():
    """
    Just for testing.
    """
    session = FastOpenData(api_key="foobar")
    data = session.request(free_form_query="1984 Lower Hawthorne Trail")
    pprint.pprint(data)
    sample_dataframe_data = []
    for _ in range(100):
        address = random_address.real_random_address_by_state("GA")
        # Commented out for development -- dev Nominatim has only Georgia
        # free_form_query = ", ".join(
        #     [address["address1"], address["address2"], address["city"]]
        # )
        free_form_query = ", ".join(
            [
                address["address1"],
                address["address2"],
            ]
        )
        sample_dataframe_data.append({"free_form_query": free_form_query})
        pprint.pprint(address)
        # data = session.request(free_form_query)
        # pprint.pprint(data)
    sample_dataframe = pd.DataFrame(sample_dataframe_data)
    session.append_to_dataframe(sample_dataframe, free_form_query="free_form_query")


if __name__ == "__main__":
    main()
