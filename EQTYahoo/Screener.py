"""
This module functions as a screener to retrieve equity tickers and their associated financial Data from Yahoo Finance.
It enables filtering equities based on various criteria such as exchange, currency, region, market capitalization,
volume, and other attributes. The retrieved Data is organized and stored in a MongoDB database, allowing for easy
querying and identification of equities that match specific conditions.
"""

import math
import datetime
import concurrent.futures
import requests
import numpy
import pandas
import re
import pymongo

# Local module import for Yahoo credentials
from . import Credentials as YFCredentials

# Retrieve Yahoo Finance credentials
Credentials = YFCredentials.Get()
Cookies = Credentials.Cookies
Crumb = Credentials.Crumb
Headers = Credentials.Headers

# Mapping from region codes to region names
RegionMapping = {
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
    "nan": "Unknown",
}


def RenameKey(Key):
    """
    Insert spaces before uppercase letters in a string and convert it to title case.

    :param Key: A string to be reformatted.
    :return: A reformatted string with spaces inserted and in title case.
    """
    SpacedKey = re.sub(r"([A-Z])", r" \1", Key)
    return SpacedKey.title()


def ScreenerBuilder(Offset, Region):
    """
    Build and execute a screener Query against Yahoo Finance for a specific region, returning up to 100 records.

    :param Offset: The zero-based index Offset for paginated Results.
    :param Region: A region code (e.g., 'us', 'ca') to filter the screener.
    :return: A JSON Response from the Yahoo Finance screener, or None on error.
    """
    URL = (
        "https://query1.finance.yahoo.com/v1/finance/screener"
        f"?formatted=false&useRecordsResponse=true&lang=en-US&crumb={Crumb}"
    )

    JSONData = {
        "size": 100,
        "offset": Offset,
        "sortType": "DESC",
        "sortField": "dayvolume",
        "includeFields": [
            "ticker",
            "companyshortname",
            "sector",
            "industry",
            "region",
            "intradayprice",
            "intradaypricechange",
            "percentchange",
            "dayvolume",
            "avgdailyvol3m",
            "intradaymarketcap",
            "peratio.lasttwelvemonths",
            "day_open_price",
            "fiftytwowklow",
            "fiftytwowkhigh",
        ],
        "topOperator": "AND",
        "query": {
            "operator": "and",
            "operands": [
                {"operator": "eq", "operands": ["region", Region]},
                {"operator": "gt", "operands": ["intradaymarketcap", 0]},
                {"operator": "gt", "operands": ["dayvolume", 0]},
                {"operator": "gt", "operands": ["avgdailyvol3m", 0]},
            ],
        },
        "quoteType": "EQUITY",
    }

    try:
        Response = requests.post(
            URL, headers=Headers, cookies=Cookies, json=JSONData
        )
        Response.raise_for_status()
        return Response.json()
    except requests.exceptions.RequestException as error:
        print(f"Request Error for region {Region}, Offset {Offset}: {error}")
        return None


def GetAllEquityTickers():
    """
    Retrieve all equity tickers from Yahoo Finance for all mapped regions,
    merge screener Data with quote Data, and return the fused dataset.
    """
    AllTickersList = []

    # Retrieve and store sector, industry information, etc.
    for RegionCode, RegionName in RegionMapping.items():
        FirstBatch = ScreenerBuilder(0, RegionCode)

        if (
            not FirstBatch
            or "finance" not in FirstBatch
            or "result" not in FirstBatch["finance"]
        ):
            continue

        TotalTickers = FirstBatch["finance"]["result"][0]["total"]
        print(f"Total tickers for {RegionName}: {TotalTickers}")

        RegionTickers = []
        # Parallel fetching for the region
        with concurrent.futures.ThreadPoolExecutor() as Executor:
            Futures = [
                Executor.submit(ScreenerBuilder, Offset, RegionCode)
                for Offset in range(0, TotalTickers, 100)
            ]
            for Future in concurrent.futures.as_completed(Futures):
                try:
                    Result = Future.result()
                    if Result:
                        records = Result["finance"]["result"][0]["records"]
                        RegionTickers.extend(records)
                        print(
                            f"Fetched {len(RegionTickers)} / {TotalTickers} for {RegionName}"
                        )
                except Exception as error:
                    print(
                        f"Error fetching tickers for region {RegionName}: {error}"
                    )

        AllTickersList.extend(RegionTickers)
        print(
            f"Completed region {RegionName}. Total tickers retrieved: {len(RegionTickers)}"
        )

    # Normalize and rename Screener fields
    KeysMapping = {
        "ticker": "Ticker",
        "companyName": "Company Name",
        "sector": "Sector",
        "region": "Region",
        "regularMarketPrice": "Price",
        "regularMarketChangePercent": "Change %",
        "regularMarketChange": "Change",
        "fiftyTwoWeekLow": "52W Low",
        "fiftyTwoWeekHigh": "52W High",
        "regularMarketVolume": "Volume",
        "avgDailyVol3m": "Avg Vol (3m)",
        "marketCap": "Market Capitalization",
        "peRatioLtm": "P/E Ratio",
        "industry": "Industry",
    }

    for TickerItem in AllTickersList:
        # Replace region code with region name
        if "region" in TickerItem:
            TickerItem["Region"] = RegionMapping.get(
                TickerItem["region"], "Unknown"
            )
            del TickerItem["region"]

        # Apply Key mapping
        for Key, NewKey in KeysMapping.items():
            if Key in TickerItem:
                TickerItem[NewKey] = TickerItem.pop(Key)

        # Convert numeric values to float64
        for Key, value in TickerItem.items():
            if isinstance(value, (float, int)):
                TickerItem[Key] = numpy.float64(value)

        # Add/Update the last update timestamp
        TickerItem["Last Update"] = datetime.datetime.timestamp(
            datetime.datetime.today().replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        )
    # --- 3) Create a mapping dictionary for quick access by Ticker
    #         Each Screener document will be copied (or referenced) to then
    #         add the quote information
    ScreenerDataMap = {}
    for Item in AllTickersList:
        # Assuming the "Ticker" key exists after mapping
        TickerSymbol = Item["Ticker"]
        ScreenerDataMap[TickerSymbol] = Item

    # Retrieve "quotes" data in batches
    TickerSymbols = list(
        ScreenerDataMap.keys()
    )  # Starting from those in the screener
    TotalLength = len(TickerSymbols)
    BatchSize = 100 #Was 1475 but isn't working anymore
    NumberOfBatches = math.ceil(TotalLength / BatchSize)

    ConsolidatedTickers = []
    for i in range(NumberOfBatches):
        StartIndex = i * BatchSize
        EndIndex = StartIndex + BatchSize
        BatchTickers = TickerSymbols[StartIndex:EndIndex]
        StringTickers = ",".join(BatchTickers)

        BaseURL = "https://query1.finance.yahoo.com/v7/finance/quote"
        URL = (
            f"{BaseURL}?symbols={StringTickers}&formatted=false&lang=en-US"
            f"&region=US&corsDomain=finance.yahoo.com&crumb={Crumb}"
        )

        try:
            ResponseData = requests.get(
                URL, headers=Headers, cookies=Cookies
            ).json()
        except Exception:
            ResponseData = {}

        QuoteResponse = ResponseData.get("quoteResponse", {}).get("result", [])
        for TickerDict in QuoteResponse:
            ConsolidatedTickers.append(TickerDict)

    # Merge Screener data with quote data
    for TickerDict in ConsolidatedTickers:
        OriginalKeys = list(TickerDict.keys())
        for Key in OriginalKeys:
            if Key == "symbol":
                TickerDict["Ticker"] = TickerDict.pop(Key)
            else:
                NewKey = RenameKey(Key)  # Rename quote keys
                TickerDict[NewKey] = TickerDict.pop(Key)

        TickerSymbol = TickerDict.get("Ticker")
        if not TickerSymbol:
            continue

        # Merge
        if TickerSymbol in ScreenerDataMap:
            ExistingData = ScreenerDataMap[TickerSymbol]

            for Key, Value in TickerDict.items():
                # Avoid overwriting the existing region if already provided by the Screener
                if Key == "Region" and "Region" in ExistingData:
                    # Do not overwrite
                    continue

                # Otherwise, overwrite or add
                ExistingData[Key] = Value
        else:
            # New ticker not present in ScreenerDataMap
            ScreenerDataMap[TickerSymbol] = TickerDict

    # Convert the final dictionary to a list
    MergedData = list(ScreenerDataMap.values())

    return MergedData


def StoreAllTickers(ConsolidatedTickers):
    """
    Store all retrieved and consolidated ticker Data in the MongoDB 'Tickers' database
    under the 'Equity' Collection.

    :param ConsolidatedTickers: A list of dictionaries representing ticker Data.
    """
    MongoClient = pymongo.MongoClient()
    DataBase = MongoClient["Tickers"]
    Collection = DataBase["Equity"]
    Collection.insert_many(ConsolidatedTickers)
    print("All Tickers have been stored in the database.")
    MongoClient.close()


def EquitiesDB():
    """
    Retrieve all equity tickers via GetAllEquityTickers() and store them in MongoDB.
    """
    Data = GetAllEquityTickers()
    StoreAllTickers(Data)


def Equity(Ticker):
    """
    Retrieve a single equity Record from the 'Equity' Collection in MongoDB by its 'Ticker'.

    :param Ticker: The ticker symbol of the equity to retrieve.
    :return: A dictionary representing the equity Data, or None if not found.
    """
    MongoClient = pymongo.MongoClient()
    DataBase = MongoClient["Tickers"]
    Collection = DataBase["Equity"]
    Record = Collection.find_one({"Ticker": Ticker})
    MongoClient.close()
    return Record


def Equities(
    Region=None,
    Sector=None,
    Industry=None,
    Market=None,
    Currency=None,
    Exchange=None,
    FullExchangeName=None,
    MinMarketCap=None,
    MaxMarketCap=None,
    MinVolume=None,
    MaxVolume=None,
):
    MongoClient = pymongo.MongoClient()
    DataBase = MongoClient["Tickers"]
    Collection = DataBase["Equity"]

    FilterList = []

    if Region is not None:
        FilterList.append({"Region": Region})

    if Sector is not None:
        FilterList.append({"Sector": Sector})

    if Industry is not None:
        FilterList.append({"Industry": Industry})

    if Market is not None:
        FilterList.append({"Market": Market})

    if Currency is not None:
        FilterList.append({"Currency": Currency})

    if Exchange is not None:
        FilterList.append({"Exchange": Exchange})

    if FullExchangeName is not None:
        FilterList.append({"Full Exchange Name": FullExchangeName})

    # MarketCap
    if MinMarketCap is not None or MaxMarketCap is not None:
        MarketCapQuery = {}
        if MinMarketCap is not None:
            MarketCapQuery["$gte"] = MinMarketCap
        if MaxMarketCap is not None:
            MarketCapQuery["$lte"] = MaxMarketCap
        FilterList.append({"Market Cap": MarketCapQuery})

    # Volume
    if MinVolume is not None or MaxVolume is not None:
        VolumeQuery = {}
        if MinVolume is not None:
            VolumeQuery["$gte"] = MinVolume
        if MaxVolume is not None:
            VolumeQuery["$lte"] = MaxVolume
        FilterList.append({"Volume": VolumeQuery})

    if len(FilterList) == 0:
        Query = {}
    else:
        Query = {"$and": FilterList}

    Results = Collection.find(Query)

    DataList = list(Results)
    MongoClient.close()

    return pandas.DataFrame(DataList)
