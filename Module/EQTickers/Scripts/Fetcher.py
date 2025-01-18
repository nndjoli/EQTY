from __future__ import print_function
import sys

import requests
import pandas as pd
import numpy as np
from ..Utils.Keys import Values

# Import the values from the Keys.py file using the Values class:
Your_GUC_Value = Values().Your_GUC_Value
Your_A1S_Value = Values().Your_A1S_Value
Your_EuConsent_Value = Values().Your_EuConsent_Value
Your_A1_Value = Values().Your_A1_Value
Your_A3_Value = Values().Your_A3_Value
Your_cmp_Value = Values().Your_cmp_Value
Your_User_Agent = Values().Your_User_Agent
Your_x_crumb_Value = Values().Your_x_crumb_Value


def GetTickers(Offset, Region):

    # Define the cookies needed for the request with placeholder values.
    Cookies = {
        "GUC": f"{Your_GUC_Value}",
        "A1S": f"{Your_A1S_Value}",
        "EuConsent": f"{Your_EuConsent_Value}",
        "A1": f"{Your_A1_Value}",
        "A3": f"{Your_A3_Value}",
        "cmp": f"{Your_cmp_Value}",
    }

    # Define the headers for the HTTP request.
    Headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        # Combine cookies into a single header string.
        "cookie": f"GUC={Your_GUC_Value}; A1S={Your_A1S_Value}; EuConsent={Your_EuConsent_Value}; A1={Your_A1_Value}; A3={Your_A3_Value}; cmp={Your_cmp_Value}",
        "origin": "https://finance.yahoo.com",
        "priority": "u=1, i",
        # The referer includes the current offset for pagination.
        "referer": f"https://finance.yahoo.com/research-hub/screener/equity/?start={Offset}&count=100",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": f"{Your_User_Agent}",
        "x-crumb": f"{Your_x_crumb_Value}",
    }

    # Define additional URL parameters for the HTTP request.
    Params = {
        "formatted": "true",
        "useRecordsResponse": "true",
        "lang": "en-US",
        "region": "US",  # Modify this if needed
        "crumb": f"{Your_x_crumb_Value}",
    }

    # Define the JSON payload for the POST request.
    JsonData = {
        "size": 100,  # Number of results per page
        "offset": Offset,  # Starting point for the results (pagination)
        "sortType": "DESC",  # Sort type: descending order
        "sortField": "dayvolume",  # Field by which to sort the results
        "includeFields": [
            "ticker",
            "sector",
            "industry",
            "region",
        ],
        "topOperator": "AND",  # Logical operator for combining query conditions
        "query": {  # Define the query filters
            "operator": "and",
            "operands": [
                {
                    "operator": "or",
                    "operands": [
                        {
                            "operator": "eq",
                            "operands": ["region", f"{Region}"],
                        },
                    ],
                },
                {
                    "operator": "or",
                    "operands": [
                        {
                            "operator": "btwn",
                            "operands": ["intradaymarketcap", 2000000000, 10000000000],
                        },
                        {
                            "operator": "btwn",
                            "operands": [
                                "intradaymarketcap",
                                10000000000,
                                100000000000,
                            ],
                        },
                        {
                            "operator": "gt",
                            "operands": ["intradaymarketcap", 100000000000],
                        },
                        {
                            "operator": "lt",
                            "operands": ["intradaymarketcap", 1000000000],
                        },
                        {
                            "operator": "lt",
                            "operands": ["intradaymarketcap", 2000000000],
                        },
                    ],
                },
                {
                    "operator": "or",
                    "operands": [
                        {
                            "operator": "lt",
                            "operands": ["dayvolume", 100000],
                        },
                        {
                            "operator": "btwn",
                            "operands": ["dayvolume", 100000, 1000000],
                        },
                        {
                            "operator": "gt",
                            "operands": ["dayvolume", 1000000],
                        },
                    ],
                },
            ],
        },
        "quoteType": "EQUITY",  # Specify that we're looking for equity data
    }

    # Send the POST request to the Yahoo Finance API using requests library.
    Response = requests.post(
        "https://query1.finance.yahoo.com/v1/finance/screener",
        params=Params,
        cookies=Cookies,
        headers=Headers,
        json=JsonData,
    )

    # Check if the response was not successful.
    if Response.status_code != 200:
        print(f"Error: {Response.status_code}")
        return None, None

    # If the response is successful, extract the total number of tickers and the records.
    if Response:
        TotalCount = Response.json()["finance"]["result"][0]["total"]
        Tickers = Response.json()["finance"]["result"][0]["records"]
        return TotalCount, Tickers


def GetTickersByRegion(Region="us"):

    # Initialize an empty list to store ticker symbols.
    TickersList = []
    # Initialize an empty list to store ticker data.
    AllTickersData = []
    # Start with an offset of 0 for pagination.
    Offset = 0
    # Get the total number of tickers for the given region.
    TotalCount, _ = GetTickers(Offset, Region)

    # Loop until we have retrieved all tickers.
    while len(TickersList) < TotalCount:
        # Break the loop if the offset exceeds the total count.
        if Offset > TotalCount:
            break
        # Fetch tickers for the current offset.
        TotalCount, OffsetData = GetTickers(Offset, Region)

        # Process each record received in the current batch.
        for Record in OffsetData:
            # If the ticker is already added, print that it is a duplicate.
            if Record["ticker"] in TickersList:
                Duplicate = f"Duplicate: {Record['ticker']}"
            else:
                # Otherwise, add the ticker symbol and its data to the lists.
                TickersList.append(Record["ticker"])
                AllTickersData.append(Record)

        # Print the current status for debugging.
        Progress = (
            f"| Region: {Region:>10} "
            f"| Total count received: {TotalCount:>6} "
            f"| Total tickers retrieved: {len(TickersList):>6} "
            f"| Current Offset: {Offset:>5} |"
        )
        sys.stdout.write("\r" + Progress)
        sys.stdout.flush()

        # Increment the offset to fetch the next batch of tickers.
        Offset += 100

    print()

    if len(AllTickersData) == 0:
        return None

    DataFrame = CleanTickersData(AllTickersData)

    return DataFrame


def GetAllTickers(RegionsList):

    print("Fetching tickers for the following regions:", RegionsList)

    DataFrame = pd.DataFrame()
    # Loop over each region in the provided list.
    for Region in RegionsList:
        # Get tickers for the current region using GetTickersbyRegion.
        RegionTickers = GetTickersByRegion(Region)

        # Skip the region if no tickers were found.
        if RegionTickers is None:
            print(f" --> No tickers were found for region: {Region:>10} ")
            continue

        # Add the tickers to the dataframe.
        DataFrame = pd.concat([DataFrame, RegionTickers], axis=0)

    return DataFrame


def CleanTickersData(AllTickers):

    # Create a DataFrame from the collected ticker data and remove duplicate rows.
    DataFrame = pd.DataFrame(AllTickers).drop_duplicates()

    # Rename and reformat columns.
    if "logoUrl" in DataFrame.columns:
        DataFrame = DataFrame.drop(columns=["logoUrl"])

    DataFrame = DataFrame.rename(
        columns={
            "ticker": "Ticker",
            "region": "Country",
            "industry": "Industry",
            "sector": "Sector",
        }
    )

    DataFrame = (
        DataFrame[
            [
                "Ticker",
                "Country",
                "Sector",
                "Industry",
            ]
        ]
        .reset_index()
        .drop(columns="index")
    )

    Mapping = {
        "ar": "Argentina",
        "at": "Austria",
        "be": "Belgium",
        "br": "Brazil",
        "ca": "Canada",
        "ch": "Switzerland",
        "cl": "Chile",
        "cn": "China",
        "cz": "Czech Republic",
        "de": "Germany",
        "dk": "Denmark",
        "fr": "France",
        "gb": "United Kingdom",
        "hk": "Hong Kong",
        "hu": "Hungary",
        "id": "Indonesia",
        "il": "Israel",
        "in": "India",
        "is": "Iceland",
        "it": "Italy",
        "jp": "Japan",
        "kr": "South Korea",
        "kw": "Kuwait",
        "mx": "Mexico",
        "nl": "Netherlands",
        "no": "Norway",
        "pl": "Poland",
        "sa": "Saudi Arabia",
        "se": "Sweden",
        "tr": "Turkey",
        "tw": "Taiwan",
        "us": "United States",
        "ve": "Venezuela",
        "za": "South Africa",
        np.nan: "Unknown",
    }

    DataFrame["Country"] = DataFrame["Country"].map(Mapping)

    # Sort the DataFrame by ticker.
    DataFrame.sort_values(by="Ticker")

    return DataFrame
