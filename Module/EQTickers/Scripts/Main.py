from pathlib import Path
import re

from .Fetcher import (
    GetAllTickers as __GetAllTickers__,
)
from ..Utils.Regions import (
    List as __AllAvailableRegions__,
)
from .BatchProcessor import (
    BatchProcessTickers as __BatchProcessTickers__,
)


def AvailableRegions():
    """
    Returns
    --------------------------------------------------------------------------
    list:                       A list of all available region identifiers as
                                strings.
    """
    return __AllAvailableRegions__


def FetchedInformations():
    """
    Returns
    --------------------------------------------------------------------------
    list:                       A list of all key informations returned by the
                                Fetching functions.
    """
    return FetchedInformationsList


def FetchRegionsTickers(RegionsList, BatchSize, WaitTime):
    """
    Fetches tickers for the specified regions and processes them in batches.

    Parameters
    --------------------------------------------------------------------------
    RegionsList (list):         A list of region identifiers or codes
                                from which to retrieve tickers.

    BatchSize (int):            The number of tickers to process in each batch.

    WaitTime (float):           The time in seconds to wait between
                                processing batches.

    Returns
    --------------------------------------------------------------------------
    pandas.DataFrame:           A DataFrame containing all processed tickers
                                and related data.
    """
    DataFrame1 = __GetAllTickers__(RegionsList)
    DataFrame2 = __BatchProcessTickers__(DataFrame1, BatchSize, WaitTime)
    return DataFrame2


def FetchAllTickers(BatchSize, WaitTime):
    """
    Fetches and processes all available equity tickers in batches.

    Parameters
    --------------------------------------------------------------------------
    BatchSize (int):            The number of tickers per batch.

    WaitTime (int or float):    The time in seconds to wait between processing
                                batches.

    Returns
    --------------------------------------------------------------------------
    pandas.DataFrame:           A DataFrame containing all processed tickers
                                from all available regions.
    """
    DataFrame1 = __GetAllTickers__(__AllAvailableRegions__)
    DataFrame2 = __BatchProcessTickers__(DataFrame1, BatchSize, WaitTime)
    return DataFrame2


FetchedInformationsList = [
    "Ask",
    "Ask Size",
    "Average Analyst Rating",
    "Average Daily Volume 10 Day",
    "Average Daily Volume 3 Month",
    "Bid",
    "Bid Size",
    "Book Value",
    "Corporate Actions",
    "Country",
    "Crypto Tradeable",
    "Currency",
    "Custom Price Alert Confidence",
    "Display Type",
    "Dividend Rate",
    "Dividend Yield",
    "EPS Current Year",
    "EPS Forward",
    "EPS Trailing Twelve Months",
    "ESG Populated",
    "Earnings Call Timestamp End",
    "Earnings Call Timestamp Start",
    "Earnings Timestamp",
    "Earnings Timestamp End",
    "Earnings Timestamp Start",
    "Exchange",
    "Exchange Data Delayed By",
    "Exchange Timezone Name",
    "Exchange Timezone Short Name",
    "Fifty Day Average",
    "Fifty Day Average Change",
    "Fifty Day Average Change Percent",
    "Fifty Two Week Change Percent",
    "Fifty Two Week High",
    "Fifty Two Week High Change",
    "Fifty Two Week High Change Percent",
    "Fifty Two Week Low",
    "Fifty Two Week Low Change",
    "Fifty Two Week Low Change Percent",
    "Fifty Two Week Range",
    "Financial Currency",
    "First Trade Date Milliseconds",
    "Forward PE",
    "Full Exchange Name",
    "GMT Offset Milliseconds",
    "Has Pre Post Market Data",
    "Industry",
    "Is Earnings Date Estimate",
    "Language",
    "Long Name",
    "Market",
    "Market Capitalization",
    "Market State",
    "Message Board ID",
    "Name Change Date",
    "Previous Name",
    "Price EPS Current Year",
    "Price Hint",
    "Price To Book",
    "Quote Source Name",
    "Quote Type",
    "Region",
    "Regular Market Change",
    "Regular Market Change Percent",
    "Regular Market Day High",
    "Regular Market Day Low",
    "Regular Market Day Range",
    "Regular Market Open",
    "Regular Market Previous Close",
    "Regular Market Price",
    "Regular Market Time",
    "Regular Market Volume",
    "Sector",
    "Shares Outstanding",
    "Short Name",
    "Source Interval",
    "Ticker",
    "Tradeable",
    "Trailing Annual Dividend Rate",
    "Trailing Annual Dividend Yield",
    "Trailing PE",
    "Triggerable",
    "Two Hundred Day Average",
    "Two Hundred Day Average Change",
    "Two Hundred Day Average Change Percent",
]


def SetKeys(new_keys: dict):
    """
    Updates the keys in Keys.py with the provided new key-value pairs.

    Parameters
    --------------------------------------------------------------------------
    new_keys (dict): Dictionary containing key names and their new values.
                      Example: {"Your_GUC_Value": "new_value", ...}
    """
    # Get the absolute path to the current directory of Main.py
    current_dir = Path(__file__).resolve().parent

    # Construct the absolute path to Keys.py
    keys_file = current_dir.parent / "Utils" / "Keys.py"

    # Check if the file exists
    if not keys_file.is_file():
        print(f"Keys.py file was not found at location: {keys_file}")
        return

    # Read the current content of Keys.py
    with keys_file.open("r", encoding="utf-8") as file:
        content = file.read()

    # Update the keys with the new values
    for key, value in new_keys.items():
        pattern = rf"(self\.{key}\s*=\s*['\"])([^'\"]*)(['\"])"
        replacement = rf"\1{value}\3"
        content, count = re.subn(pattern, replacement, content)
        if count == 0:
            print(f"Key '{key}' was not found.")

    # Write the updated content to Keys.py
    with keys_file.open("w", encoding="utf-8") as file:
        file.write(content)

    print("Keys updated successfully.")


def GetKeys():
    """
    Returns
    --------------------------------------------------------------------------
    dict:                       A dictionary containing the key-value pairs
                                from Keys.py.
    """
    # Get the absolute path to the current directory of Main.py
    current_dir = Path(__file__).resolve().parent

    # Construct the absolute path to Keys.py
    keys_file = current_dir.parent / "Utils" / "Keys.py"

    # Check if the file exists
    if not keys_file.is_file():
        print(f"Keys.py file was not found at location: {keys_file}")
        return

    # Read the current content of Keys.py
    with keys_file.open("r", encoding="utf-8") as file:
        content = file.read()

    # Extract the key-value pairs from the content
    keys = {}
    for line in content.splitlines():
        match = re.match(r"^\s*self\.(\w+)\s*=\s*['\"](.*)['\"]", line)
        if match:
            key, value = match.groups()
            keys[key] = value

    return keys


def ResetKeys():

    # Get the absolute path to the current directory of Main.py
    current_dir = Path(__file__).resolve().parent

    # Construct the absolute path to Keys.py
    keys_file = current_dir.parent / "Utils" / "Keys.py"

    # Check if the file exists
    if not keys_file.is_file():
        print(f"Keys.py file was not found at location: {keys_file}")
        return

    # Read the current content of Keys.py
    with keys_file.open("r", encoding="utf-8") as file:
        content = file.read()

    # Define the pattern and replacement
    pattern = r"(self\.\w+\s*=\s*['\"])([^'\"]*)(['\"])"
    replacement = r"\1insert_your_value\3"

    # Update the content
    updated_content, count = re.subn(pattern, replacement, content)

    # Write the updated content to Keys.py
    with keys_file.open("w", encoding="utf-8") as file:
        file.write(updated_content)

    print(f"Reset done successfully. {count} keys were updated.")
