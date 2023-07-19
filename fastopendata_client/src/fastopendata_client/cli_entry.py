import click
import json
import logging
import os
import re
import sys

from pyfiglet import Figlet
from fastopendata_client.client import FastOpenData


logging.basicConfig(level=logging.INFO)
    
FIGLET = Figlet(font='slant')
BANNER = FIGLET.renderText("FastOpenData")

def check_api_key(api_key):
    '''
    Check that API key is either specified or defined in an
    environment variable.
    '''
    api_key = api_key or os.environ.get("FASTOPENDATA_API_KEY", None)
    if not api_key:
        print(
            "You need to specify an API key using the option "
            '"--api-key" or export your API key to the environment '
            'variable "FASTOPENDATA_API_KEY".'
            "\n"
            "\n"
            "To get a free API key for evaluation purposes and simple "
            "use-cases, try the command: "
            "\n"
            "\n"
            "fastopendata get-api-key"
        )
        sys.exit(1)
    return api_key

@click.group()
def cli_entry(
):
    '''This is the command-line tool for FastOpenData.

    Use it to get an API key, retrieve data for a single address,
    or append data to an existing CSV file.
    '''
    pass

@cli_entry.command()
@click.option(
    "--free-form-query", default=None, help="unstructured United States address"
)
@click.option("--address1", default=None, help="street address line one")
@click.option("--address2", default=None, help="street address line two")
@click.option("--city", default=None, help="city")
@click.option("--state", default=None, help="state")
@click.option("--zip-code", default=None, help="zip code")
@click.option("--api-key", default=None, help="API key")
def get(free_form_query, address1, address2, city, state, zip_code, api_key):
    '''Get a single data payload for one address.'''
    api_key = check_api_key(api_key)

    def _unstructured_address_provided():
        return bool(free_form_query)
    
    def _structured_address_provided():
        return bool(address1) and bool(city) and bool(state) and bool(zip_code)

    def _too_many_addresses_provided():
        return _unstructured_address_provided() and _structured_address_provided()
        
    
    if _too_many_addresses_provided():
        print(
            'You can use either --free-form-query or the structured address '
            'parameters, but not both.'
        )
        sys.exit(1)
    if not _unstructured_address_provided() and not _structured_address_provided():
        print(
            'You must specify an address. Use "fastopendata --help" for options.'
        )
        sys.exit(1)
    logging.debug(f'fastopendata got query: {free_form_query}')
    client = FastOpenData(api_key=api_key)
    data = client.request(free_form_query=free_form_query)
    if data:
        print(json.dumps(data, indent=2))
    else:
        print('Some kind of error')
        sys.exit(1)


@cli_entry.command()
@click.option("--api-key", default=None, help="API key")
@click.option("--input-csv", default=None, help="input CSV file with addresses")
@click.option("--output-csv", default=None, help="target CSV with appended data")
@click.option(
    "--free-form-query", default=None, help="unstructured United States address"
)
@click.option("--address1", default=None, help="street address line one")
@click.option("--address2", default=None, help="street address line two")
@click.option("--city", default=None, help="city")
@click.option("--state", default=None, help="state")
@click.option("--zip-code", default=None, help="zip code")
@click.option("--api-key", default=None, help="API key")
def csv(api_key, input_csv, output_csv, free_form_query, address1, address2, city, state, zip_code):
    '''
    Append data from FastOpenData to an existing CSV file.
    '''
    api_key = check_api_key(api_key)
    if not input_csv:
        print('You must provide the path of a CSV file.')
        sys.exit(1)
    if not output_csv:
        print('You must provide the path for the output CSV.')
        sys.exit(1)
    if not os.path.isfile(input_csv): 
        print(f'CSV file {input_csv} does not exist.')
        sys.exit(1)
    if os.path.isfile(output_csv): 
        print(f'Output file {output_csv} already exists.')
        sys.exit(1)
    pass


@cli_entry.command()
def get_api_key():
    '''
    Get a free API key for FastOpenData.
    '''
    email_address = click.prompt('Enter your email address')
    email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
 
    if(not re.fullmatch(email_regex, email_address)):
        print(f'Email {email_address} is not valid.')
        return 

    response_dict = FastOpenData.get_free_api_key(email_address=email_address)
    
    if response_dict['status'] == 'EXPIRE_OLD_KEY' or response_dict['status'] == 'SUCCESS':
        api_key = response_dict['api_key']
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
    if response_dict['status'] == 'EXPIRE_OLD_KEY':
        print(
            '\n'
            'Note: This email address already had an API key. The old one will be expired.'
        )


if __name__ == "__main__":
    cli_entry()
