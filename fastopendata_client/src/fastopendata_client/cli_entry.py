import click
import json
import logging
import os
import sys

from pyfiglet import Figlet
from client import FastOpenData


logging.basicConfig(level=logging.INFO)
    
FIGLET = Figlet(font='slant')
BANNER = FIGLET.renderText("FastOpenData")

# fix later
# print(BANNER)

@click.command()
@click.option(
    "--free-form-query", default=None, help="unstructured United States address"
)
@click.option("--address1", default=None, help="street address line one")
@click.option("--address2", default=None, help="street address line two")
@click.option("--city", default=None, help="city")
@click.option("--state", default=None, help="state")
@click.option("--zip-code", default=None, help="zip code")
@click.option("--api-key", default=None, help="API key")
@click.argument("command")
def cli_entry(
    free_form_query, address1, address2, city, state, zip_code, api_key, command
):
    '''This is the command-line tool for FastOpenData.

    You can do many wonderful things with it. You can, for example, get a single address-worth
    of data.
    '''
    if command == "get":
        api_key = api_key or os.environ.get("FASTOPENDATA_API_KEY", None)
        def _unstructured_address_provided():
            return bool(free_form_query)
        
        def _structured_address_provided():
            return bool(address1) and bool(city) and bool(state) and bool(zip_code)

        def _too_many_addresses_provided():
            return _unstructured_address_provided() and _structured_address_provided()
            
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
            sys.exit(0)
        if _too_many_addresses_provided():
            print(
                'You can use either --free-form-query or the structured address '
                'parameters, but not both.'
            )
            sys.exit(0)
        if not _unstructured_address_provided() and not _structured_address_provided():
            print(
                'You must specify an address. Use "fastopendata --help" for options.'
            )
            sys.exit(0)
        logging.debug(f'fastopendata got query: {free_form_query}')
        client = FastOpenData(api_key=api_key)
        data = client.request(free_form_query=free_form_query)
        if data:
            print(json.dumps(data, indent=2))
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    cli_entry()
