import logging
import pprint

import numpy as np
import pandas as pd
import random_address
import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(level=logging.INFO)


class FastOpenDataSecurityException(Exception):
    pass


class FastOpenDataClientException(Exception):
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
    ) -> None:
        """
        Call FastOpenData for each row in the DataFrame. Append
        new columns to the DataFrame containing the resulting
        data.
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
        elif match_mode == 'STRUCTURED_QUERY':
            raise FastOpenDataClientException(
                "Haven't gotten to this yet."
            )
        if df.empty:
            raise FastOpenDataClientException(
                'Passed an empty DataFrame to append function.'
            )
        row_counter = 0

        # We waste a few nanoseconds here, but it's easier for
        # my addled mind to track with this private function.
        def _flatten_response(response_dict: dict) -> dict:
            '''
            Flatten the keys for the `response_dict` so that
            the values can be appended to the DataFrame.
            '''
            flat_response_dict = {}
            for geography, subdict in response_dict.items():
                for attribute, value in subdict.items():
                    column_name = '.'.join([geography, attribute])
                    flat_response_dict[column_name] = value
            return flat_response_dict
        
        for index, row in df.iterrows():
            if match_mode == 'FREE_FORM_QUERY':
                request_params = {
                    'free_form_query': row[free_form_query],
                }
            elif match_mode == 'STRUCTURED_QUERY':
                request_params = {
                    'address1': row[address1],
                    'address2': row[address2],
                    'city': row[city],
                    'state': row[state],
                    'zip_code': row[zip_code],
                }
            else:
                raise Exception('This should never happen.')
            response_dict = self.request(**request_params)
            if response_dict is None:
                continue
            flat_response = _flatten_response(response_dict)
            if row_counter == 0:
                # First row is special because we need to get all
                # the column names.
                data_column_list = [column_name for column_name in flat_response.keys()]
                # Finally add the columns with `np.NaN` values everywhere
                for column_name in data_column_list:
                    df.insert(
                        len(list(df.columns)),
                        column_name, np.NaN
                    )
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
    for _ in range(100):
        address = random_address.real_random_address_by_state("GA")
        # Commented out for development -- dev Nominatim has only Georgia
        # free_form_query = ", ".join(
        #     [address["address1"], address["address2"], address["city"]]
        # )
        free_form_query = ", ".join(
            [address["address1"], address["address2"],]
        )
        sample_dataframe_data.append({'free_form_query': free_form_query})
        pprint.pprint(address)
        data = session.request(free_form_query)
        pprint.pprint(data)
    sample_dataframe = pd.DataFrame(sample_dataframe_data)
    session.append_to_dataframe(sample_dataframe, free_form_query='free_form_query')