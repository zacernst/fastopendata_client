import json
import logging
import os
import re
import sys
from typing import Optional

import click
import rich
from pyfiglet import Figlet
from rich.console import Group
from rich.panel import Panel
from rich.syntax import Syntax

from fastopendata_client.client import FastOpenData, FastOpenDataConnectionException

logging.basicConfig(level=logging.INFO)

FIGLET = Figlet(font="slant")
BANNER = FIGLET.renderText("FastOpenData")
ADDRESS_DATA_BATCH_SIZE = 100


def check_api_key(api_key):
    """
    Check that API key is either specified or defined in an
    environment variable.
    """
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
def cli_entry():
    """
    This is the command-line tool for FastOpenData. \n
    
    Use it to get an API key, retrieve data for a single address,
    or append data to an existing CSV file.

    """



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
@click.option("--no-pretty-print", is_flag=True, default=False, help="Suppress pretty-printing the JSON response")
def get(
    free_form_query: Optional[str], 
    address1: Optional[str], 
    address2: Optional[str], 
    city: Optional[str], 
    state: Optional[str], 
    zip_code: Optional[str], 
    api_key: Optional[str], 
    no_pretty_print: Optional[bool]
):
    """Get a single data payload for one address."""
    api_key = check_api_key(api_key)

    def _unstructured_address_provided():
        return bool(free_form_query)

    def _structured_address_provided():
        return bool(address1) and bool(city) and bool(state) and bool(zip_code)

    def _too_many_addresses_provided():
        return _unstructured_address_provided() and _structured_address_provided()

    if _too_many_addresses_provided():
        print(
            "You can use either --free-form-query or the structured address "
            "parameters, but not both."
        )
        sys.exit(1)
    if not _unstructured_address_provided() and not _structured_address_provided():
        print('You must specify an address. Use "fastopendata --help" for options.')
        sys.exit(1)
    logging.debug(f"fastopendata got query: {free_form_query}")
    client = FastOpenData(api_key=api_key)
    data = client.request(free_form_query=free_form_query)
    if not data:
        print("No data found for the address.")
        sys.exit(1)
    if no_pretty_print:
        print(json.dumps(data))
    else:
        rich.print(data)


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
def csv(
    api_key,
    input_csv,
    output_csv,
    free_form_query,
    address1,
    address2,
    city,
    state,
    zip_code,
):
    """
    Append data from FastOpenData to an existing CSV file.
    """
    api_key = check_api_key(api_key)

    client = FastOpenData(api_key=api_key)
    client.append_to_csv(
        input_csv=input_csv,
        output_csv=output_csv,
        free_form_query=free_form_query,
        address1=address1,
        address2=address2,
        city=city,
        state=state,
        zip_code=zip_code,
    )


@cli_entry.command()
def get_api_key():
    """
    Get a free API key for FastOpenData.
    """
    email_address = click.prompt("Enter your email address")
    email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b"

    if not re.fullmatch(email_regex, email_address):
        print(f"Email {email_address} is not valid.")
        return

    try:
        response_dict = FastOpenData.get_free_api_key(email_address=email_address)
    except FastOpenDataConnectionException as e:
        rich.print(
            '[red]Error: Unable to connect to the FastOpenData server.[/red] '
            'This may indicate either a problem with the FastOpenData server, or '
            'a problem with your internet connection.'
        )
        sys.exit(1)

    if (
        response_dict["status"] == "EXPIRE_OLD_KEY"
        or response_dict["status"] == "SUCCESS"
    ):
        api_key = response_dict["api_key"]
        python_code = """
from fastopendata_client import FastOpenData
session = FastOpenData(api_key="{api_key}")
session.request(
    free_form_query="1984 Lower Hawthorne Trail, Cairo, GA 39828"
)
""".format(
            api_key=api_key
        )

        bash_code = """
fastopendata get --api-key="{api_key}" \\
    --free-form-query="1984 Lower Hawthorne Trail"
""".format(
            api_key=api_key
        )
        msg_1 = """
Your API key is:\n
[bold]{api_key}[/bold]\n
To test your client using the command-line tool, try:
""".format(
            api_key=api_key
        )

        msg_2 = """If you want to try writing some Python code:"""

        msg_3 = """If you don't want to keep entering your API key, you can set an environment variable `FASTOPENDATA_API_KEY`."""

        msg_4 = """For API documentation, visit <https://fastopendata.com>"""

        group = Group(
            msg_1,
            Syntax(bash_code, "bash", theme="dracula"),
            msg_2,
            Syntax(python_code, "python", theme="dracula", line_numbers=False),
            # msg_3,
            msg_4,
        )
        rich.print(Panel(group, title="[green]Success!", subtitle=None))
    if response_dict["status"] == "EXPIRE_OLD_KEY":
        rich.print(
            "[red]Note: This email address already had an API key. The old one will be expired.[/red]"
        )


if __name__ == "__main__":
    cli_entry()
