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
import pprint

import numpy as np
import os
import pandas as pd
import random_address
import requests
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)


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


class FastOpenData:
    """
    The client for interacting with the FastOpenData service.
    """

    def __init__(
        self,
        ip_address: str = "localhost",
        port: int = 8000,
        scheme: str = "http",
        api_key: str = None,
    ) -> None:
        self.api_key = api_key
        self.ip_address = ip_address
        self.port = str(port)
        self.scheme = scheme
        if not self.api_key:
            raise FastOpenDataSecurityException(
                "Must specify an API key when instantiating the `FastOpenData` class "
                "by passing `api_key=YOUR_API_KEY`."
            )
        self.url = f"{self.scheme}://{self.ip_address}:{self.port}"

    @staticmethod
    def get_free_api_key(email_address: str) -> str:
        """
        Get a free API key from FastOpenData for evaluation purposes.
        """
        free_api_key_url = f"http://localhost:8000/get_free_api_key"
        headers = {
            "Content-type": "application/json",
        }
        response = requests.get(
            free_api_key_url, params={"email_address": email_address}, headers=headers
        )
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise e
        api_key = response.json()["api_key"]
        print(
            "Your API key is:\n"
            f"{api_key}\n"
            "\n"
            "To test your client, try:\n"
            ">>> from fastopendata_client import FastOpenData\n"
            f'>>> session = FastOpenData(api_key="{api_key}")\n'
            '>>> session.request(free_form_query="1984 Lower Hawthorne Trail, Cairo, GA 39828")\n'
            "\n"
            "You should receive a dictionary with a lot of data about that address.\n"
            "\n"
            "Questions? Contact zac@fastopendata.com"
        )

    def request(
        self,
        free_form_query: str = None,
        city: str = None,
        state: str = None,
        address1: str = None,
        address2: str = None,
        zip_code: str = None,
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
        if not (free_form_query or city or state or address or zip_code):
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
            "x-api-key": self.api_key,
        }
        response = requests.get(
            self.url, params={"free_form_query": free_form_query}, headers=headers
        )

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise e

        return response.json()

    def append_to_dataframe(
        self,
        df: pd.DataFrame,
        free_form_query: str = "free_form_query",
        address1: str = "address1",
        address2: str = "address2",
        city: str = "city",
        state: str = "state",
        zip_code: str = "zip_code",
        progressbar: bool = True,
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




if __name__ == "__main__":
    """
    Just for testing.
    """
    session = FastOpenData(api_key="foobar")
    data = session.request("1984 Lower Hawthorne Trail")
    pprint.pprint(data)
    sample_dataframe_data = []
    import pdb; pdb.set_trace()
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