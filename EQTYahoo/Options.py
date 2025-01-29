"""
This module retrieves and processes options chain data for a given ticker from Yahoo Finance.
It fetches expiration dates, call and put options, and calculates derived metrics like moneyness.
Cleaned data is stored in a MongoDB database with mechanisms to ensure regular updates based on market hours.
"""

import datetime
import pymongo
import requests
import re
from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd

# Local module import for Yahoo credentials
from . import Credentials as YFCredentials

# Retrieve credentials for Yahoo Finance
Credentials = YFCredentials.Get()
Cookies = Credentials.Cookies
Crumb = Credentials.Crumb
Headers = Credentials.Headers


def RenameKey(Key):
    """
    Insert spaces before uppercase letters and convert to title case.

    :param Key: The original key string.
    :return: The renamed key with spaces inserted and in title case.
    """
    SpacedKey = re.sub(r"([A-Z])", r" \1", Key)
    return SpacedKey.title()


def GetResponseSub(Ticker, ExpirationDate, Crumb):
    """
    Fetch options data for a specific expiration date.

    :param Ticker: The ticker symbol.
    :param ExpirationDate: The expiration date timestamp.
    :param Crumb: The crumb token for Yahoo Finance API.
    :return: JSON response for the specific expiration date or None if an error occurs.
    """
    ExpirationUrl = (
        f"https://query1.finance.yahoo.com/v7/finance/options/{Ticker}"
        f"?date={ExpirationDate}&lang=en-US&region=US&corsDomain=finance.yahoo.com&crumb={Crumb}"
    )
    try:
        Response = requests.get(
            ExpirationUrl, headers=Headers, cookies=Cookies
        ).json()
        return Response
    except Exception as E:
        print(f"Error fetching expiration {ExpirationDate}: {E}")
        return None


def GetJsonResponse(Ticker):
    """
    Retrieve options data for a given ticker using Yahoo Finance API
    and assemble a list of all calls/puts for each expiration date.

    :param Ticker: The ticker symbol to retrieve options data for.
    :return: A list of option dictionaries with renamed keys and additional underlying information.
    """
    try:
        # Fetch base data (quote + initial expirations)
        Url = (
            f"https://query1.finance.yahoo.com/v7/finance/options/{Ticker}"
            f"?lang=en-US&region=US&corsDomain=finance.yahoo.com&crumb={Crumb}"
        )
        Response = requests.get(Url, headers=Headers, cookies=Cookies).json()
    except Exception as E:
        raise Exception(f"Error: Could not fetch data for ticker {Ticker}: {E}")

    Quote = Response["optionChain"]["result"][0]["quote"]
    Ticker = Quote["symbol"]
    ExpirationDates = Response["optionChain"]["result"][0]["expirationDates"]

    # Fetch data for each expiration date (parallelized)
    PoolInstance = ThreadPool(100)
    PerExpirations = PoolInstance.starmap(
        GetResponseSub, [(Ticker, Date, Crumb) for Date in ExpirationDates]
    )
    PoolInstance.close()
    PoolInstance.join()

    # Build the complete list of call/put options
    Chains = []
    for ExpirationData in PerExpirations:
        if ExpirationData is None:
            continue

        OptionsData = ExpirationData["optionChain"]["result"][0]["options"][0]

        for Option in OptionsData.get("calls", []):
            Option["Type"] = "Call"
            Chains.append(Option)

        for Option in OptionsData.get("puts", []):
            Option["Type"] = "Put"
            Chains.append(Option)

    # Rename keys and add underlying information
    for Option in Chains:
        OriginalKeys = list(Option.keys())
        for Key in OriginalKeys:
            if Key != "Type":
                NewKey = RenameKey(Key)
                Option[NewKey] = Option.pop(Key)

        Option["Underlying Name"] = Quote.get("shortName")
        Option["Underlying Region"] = Quote.get("region")
        Option["Underlying Ticker"] = Quote.get("symbol")
        Option["Underlying Volume"] = Quote.get("regularMarketVolume")
        Option["Underlying Open Price"] = Quote.get("regularMarketOpen")
        Option["Underlying High Price"] = Quote.get("regularMarketDayHigh")
        Option["Underlying Low Price"] = Quote.get("regularMarketDayLow")
        Option["Underlying Price"] = Quote.get("regularMarketPrice")
        Option["Underlying Currency"] = Quote.get("currency")
        Option["Underlying Exchange"] = Quote.get("fullExchangeName")
        Option["Underlying Type"] = Quote.get("typeDisp")
        Option["Underlying Quote Source"] = Quote.get("quoteSourceName")
        Option["Underlying Dividend Yield"] = Quote.get("dividendYield")

    return Chains


def CleanedJsonResponse(JsonResponse):
    """
    Filter and clean the list of options by avoiding modifications during iteration
    and removing unnecessary fields.

    :param JsonResponse: The raw list of option dictionaries.
    :return: A cleaned list of option dictionaries.
    """
    NowTimestamp = datetime.datetime.now().timestamp()
    CleanedData = []

    for Option in JsonResponse:
        # Skip if the expiration date has already passed
        if Option["Expiration"] < NowTimestamp:
            continue

        # Skip if Last Price or Strike is 0
        if Option["Last Price"] == 0 or Option["Strike"] == 0:
            continue

        # Safely remove fields if they exist
        Option.pop("Implied Volatility", None)
        Option.pop("In The Money", None)

        # Rename contract-related fields
        Option["Contract Strike"] = Option.pop("Strike", None)
        Option["Contract Type"] = Option.pop("Type", None)
        Option["Contract Expiration"] = Option.pop("Expiration", None)
        Option["Contract Last Price"] = Option.pop("Last Price", None)
        Option["Contract Open Interest"] = Option.pop("Open Interest", None)
        Option["Contract Volume"] = Option.pop("Volume", None)
        Option["Contract Bid"] = Option.pop("Bid", None)
        Option["Contract Ask"] = Option.pop("Ask", None)
        Option["Contract Change"] = Option.pop("Change", None)
        Option["Contract Percent Change"] = Option.pop("Percent Change", None)
        Option["Contract Currency"] = Option.pop("Currency", None)

        # Calculate moneyness
        if Option["Contract Type"] == "Call":
            Option["Moneyness Formula"] = "(S/K)"
            Option["Contract Moneyness"] = (
                Option["Underlying Price"] / Option["Contract Strike"]
                if Option["Contract Strike"]
                else None
            )
        elif Option["Contract Type"] == "Put":
            Option["Moneyness Formula"] = "(K/S)"
            Option["Contract Moneyness"] = (
                Option["Contract Strike"] / Option["Underlying Price"]
                if Option["Underlying Price"]
                else None
            )
        else:
            Option["Moneyness Formula"] = None
            Option["Contract Moneyness"] = None

        # Mark the last update timestamp
        Option["Last Update"] = datetime.datetime.now().timestamp()

        CleanedData.append(Option)

    return CleanedData


def StoreOptionsChains(CleanedJsonResponse):
    """
    Store the cleaned options data in MongoDB.

    :param CleanedJsonResponse: The cleaned list of option dictionaries.
    """
    if not CleanedJsonResponse:
        print("No cleaned data to store.")
        return

    Ticker = CleanedJsonResponse[0].get("Underlying Ticker")
    if not Ticker:
        print("No ticker information found in the data.")
        return

    Client = pymongo.MongoClient()
    Db = Client["Options"]
    Collection = Db[Ticker]

    # Replace existing documents with upsert to avoid duplication
    for Option in CleanedJsonResponse:
        Collection.replace_one(
            {
                "Contract Expiration": Option["Contract Expiration"],
                "Contract Strike": Option["Contract Strike"],
                "Contract Type": Option["Contract Type"],
            },
            Option,
            upsert=True,
        )

    Client.close()


def Chain(
    Ticker: str,
    ContractsType: str = None,
    StrikeRange: list = None,
    MoneynessRange: list = None,
    OpenInterestRange: list = None,
    VolumeRange: list = None,
    LastPriceRange: list = None,
    ThirdFridaysOnly: bool = False,
):
    """
    Check if data is already present in the database. If so, verify the last update date
    and decide whether to re-download based on current time and day. Otherwise, download and store.

    :param Ticker: The ticker symbol to retrieve options data for.
    :param ContractsType: Filter by contract type ('Call' or 'Put').
    :param StrikeRange: List containing min and max strike prices.
    :param MoneynessRange: List containing min and max moneyness values.
    :param OpenInterestRange: List containing min and max open interest.
    :param VolumeRange: List containing min and max volume.
    :param LastPriceRange: List containing min and max last price.
    :param ThirdFridaysOnly: Boolean to filter only options expiring on third Fridays.
    :return: A pandas DataFrame containing the filtered options data.
    """
    Client = pymongo.MongoClient()
    Db = Client["Options"]
    Collection = Db[Ticker]

    ExistingData = Collection.find_one()
    if ExistingData is None:
        print(f"Data for {Ticker} not found. Downloading...")
        Options = GetJsonResponse(Ticker)
        CleanedOptions = CleanedJsonResponse(Options)
        StoreOptionsChains(CleanedOptions)
        Client.close()
        return pd.DataFrame(CleanedOptions)

    LastUpdate = Collection.find_one(sort=[("Last Update", -1)]).get(
        "Last Update"
    )
    Now = datetime.datetime.now().timestamp()

    # Get current day, hour, and minute (UTC) to determine if the market is open
    NowDt = datetime.datetime.now(tz=datetime.timezone.utc)
    NowDay = NowDt.isoweekday()
    NowHour = NowDt.hour
    NowMinute = NowDt.minute

    # Update frequency based on market hours
    if (
        NowDay <= 5
        and (NowHour > 9 or (NowHour == 9 and NowMinute >= 30))
        and (NowHour < 16 or (NowHour == 16 and NowMinute <= 30))
    ):
        UpdateFrequencySeconds = 15 * 60  # 15 minutes
    else:
        UpdateFrequencySeconds = (
            60 * 60 * 24
        )  # 24 hours if the market is closed

    if Now - LastUpdate > UpdateFrequencySeconds:
        print(f"Data for {Ticker} is outdated. Updating...")
        Collection.delete_many({})
        Options = GetJsonResponse(Ticker)
        CleanedOptions = CleanedJsonResponse(Options)
        StoreOptionsChains(CleanedOptions)
        Client.close()
        return pd.DataFrame(CleanedOptions)

    print(
        f"{Ticker} Options Chain -- Last updated: {datetime.datetime.fromtimestamp(LastUpdate)}"
    )
    Data = list(Collection.find())
    Client.close()

    NowDate = datetime.datetime.now().date()
    In10Years = NowDate + datetime.timedelta(days=3650)
    ThirdFridays = [
        Date.to_pydatetime().date()
        for Date in pd.date_range(start=NowDate, end=In10Years, freq="WOM-3FRI")
    ]

    for Option in Data:
        Option["Last Trade Date"] = datetime.datetime.fromtimestamp(
            Option["Last Trade Date"]
        )
        Option["Last Update"] = datetime.datetime.fromtimestamp(
            Option["Last Update"]
        )
        Option["Contract Expiration"] = datetime.datetime.fromtimestamp(
            Option["Contract Expiration"]
        ).date()
        Option["Third Friday"] = Option["Contract Expiration"] in ThirdFridays

    Dataframe = pd.DataFrame(Data)

    if ThirdFridaysOnly:
        Dataframe = Dataframe[Dataframe["Third Friday"] == True]

    if MoneynessRange is not None:
        Dataframe = Dataframe[
            (Dataframe["Contract Moneyness"] >= MoneynessRange[0])
            & (Dataframe["Contract Moneyness"] <= MoneynessRange[1])
        ]

    if StrikeRange is not None:
        Dataframe = Dataframe[
            (Dataframe["Contract Strike"] >= StrikeRange[0])
            & (Dataframe["Contract Strike"] <= StrikeRange[1])
        ]

    if ContractsType is not None:
        Dataframe = Dataframe[Dataframe["Contract Type"] == ContractsType]

    if OpenInterestRange is not None:
        Dataframe = Dataframe[
            (Dataframe["Contract Open Interest"] >= OpenInterestRange[0])
            & (Dataframe["Contract Open Interest"] <= OpenInterestRange[1])
        ]

    if VolumeRange is not None:
        Dataframe = Dataframe[
            (Dataframe["Contract Volume"] >= VolumeRange[0])
            & (Dataframe["Contract Volume"] <= VolumeRange[1])
        ]

    if LastPriceRange is not None:
        Dataframe = Dataframe[
            (Dataframe["Contract Last Price"] >= LastPriceRange[0])
            & (Dataframe["Contract Last Price"] <= LastPriceRange[1])
        ]

    return Dataframe
