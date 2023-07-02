FastOpenData Client
===================

The Python client for FastOpenData.

To use this client to retrieve data, you must have an API key. You can use the
client itself to get a free API key that's suitable for evaluation purposes:

```
>>> from fastopendata_client import FastOpenData
>>> api_key = FastOpenData.get_api_key('YOUR_EMAIL_ADDRESS')
```

Save your API key somewhere; you will use it each time you invoke the FastOpenData
client. If you lose your key, you can call this method again with the same
email address.

The free API key is rate limited and not suitable for production purposes. To
subscribe to the FastOpenData service and receive a key that will provide unlimited
access, please visit https://fastopendata.com. If you have questions about the
service or your particular use-case, email zac@fastopendata.com.

The main class for this client is `FastOpenData` which is invoked like so:

```
>>> from fastopendata_client import FastOpenData
>>> session = FastOpenData(api_key="<YOUR_API_KEY>")
>>> data = session.request(free_form_query="123 Main Street, Tallahassee, FL, 12345")
```

`data` now contains a dictionary with all the data from the FastOpenData server.

If you have a Pandas dataframe, you can append new columns containing data from
FastOpenData by calling `FastOpenData.append_to_dataframe` and specifying which
columns contain address information. For example, if `COLUMN_NAME` contains
unstructured address strings, you can do this:

```
>>> import pandas as pd
>>> from fastopendata_client import FastOpenData
>>> session = FastOpenData(api_key="<YOUR_API_KEY>")
>>> df = pd.DataFrame(...)
>>> session.append_to_dataframe(df, free_form_query=COLUMN_NAME)
```

Now `df` contains many new columns containing data from the FastOpenData server.

If your dataframe has columns containing structured address information, you
can do:

```
>>> session.append_to_dataframe(
        df,
        address1=ADDRESS1_COLUMN,
        address2=ADDRESS2_COLUMN,
        city=CITY_COLUMN,
        state=STATE_COLUMN,
        zip_code=ZIP_CODE_COLUMN
    )
```

Note that you have the option of specifying either `free_form_query` or the column names for structured address data, but not both. Doing so will raise an exception.