"""
This module retrieves historical market data from Yahoo Finance, processes it into a structured
format, and stores it in a MongoDB database. It supports querying data for single or multiple 
tickers, with configurable granularity and date ranges, and ensures data consistency by updating 
missing or outdated records.
"""

import numpy
import pandas
import pymongo
import requests
import datetime
from . import Credentials as YFCredentials


Credentials = YFCredentials.Get()
Headers = Credentials.Headers
Cookies = Credentials.Cookies


def GetJSONResponse(
    Ticker: str,
    Period: str = None,
    Granularity: str = "1d",
    StartDate: str = None,
    EndDate: str = None,
    Logs: bool = False,
) -> dict:
    """
    Fetches JSON response from Yahoo Finance API for a given ticker symbol.
    Args:
        Ticker (str): The ticker symbol for the financial instrument.
        Period (str, optional): The period for which data is requested (e.g., '1d', '5d', '1mo'). Default is None.
        Granularity (str, optional): The data granularity (e.g., '1d', '1wk', '1mo'). Default is '1d'.
        StartDate (str, optional): The start date for the data in 'YYYY-MM-DD' format. Default is None.
        EndDate (str, optional): The end date for the data in 'YYYY-MM-DD' format. Default is None.
        Logs (bool, optional): Flag to enable logging. Default is False.
    Returns:
        dict: A dictionary containing the JSON response data from Yahoo Finance API.
    Raises:
        ValueError: If required parameters are missing or invalid.
    """

    # Validate required parameters
    if Ticker is None:
        raise ValueError("Ticker is required.")
    if Period is None and StartDate is None and EndDate is None:
        raise ValueError(
            "Either 'Period' or 'StartDate'/'EndDate' must be provided."
        )

    # Convert dates to timestamps
    NowTimestamp = int(pandas.Timestamp.now().timestamp())
    StartTimestamp = (
        int(pandas.Timestamp(StartDate).timestamp()) if StartDate else None
    )
    EndTimestamp = (
        int(pandas.Timestamp(EndDate).timestamp()) if EndDate else None
    )

    # Validate date range
    if StartTimestamp and EndTimestamp and StartTimestamp >= EndTimestamp:
        raise ValueError("StartDate must be earlier than EndDate.")

    # Construct the query URL
    BaseURL = "https://query2.finance.yahoo.com/v8/finance/chart/"
    QueryParameters = f"{Ticker}?"

    if StartTimestamp and EndTimestamp:
        QueryParameters += f"&period1={StartTimestamp}&period2={EndTimestamp}"
    elif StartTimestamp:
        QueryParameters += f"&period1={StartTimestamp}&period2={NowTimestamp}"
    elif Period:
        QueryParameters += f"&range={Period}"
    else:
        raise ValueError(
            "Invalid date range. Provide 'Period' or 'StartDate'/'EndDate'."
        )

    QueryParameters += f"&interval={Granularity}"
    QueryParameters += "&events=history,div,splits"
    URL = f"{BaseURL}{QueryParameters}"

    # Make the request to Yahoo Finance API
    Response = requests.get(URL, headers=Headers, cookies=Cookies)

    # Check for errors in the response
    if Response.status_code != 200:
        ErrorDescription = (
            Response.json()
            .get("chart", {})
            .get("error", {})
            .get("description", "Unknown error")
        )
        raise ValueError(f"{ErrorDescription}")

    # Parse and return the response data
    try:
        Data = Response.json()["chart"]["result"][0]
        Data["Request_StartTimestamp"] = StartTimestamp
        Data["Request_EndTimestamp"] = EndTimestamp
        return Data
    except (KeyError, IndexError, TypeError) as ExtractError:
        raise ValueError(f"Error parsing response data: {ExtractError}")


def GetCleanedJSONResponse(JSONResponse):
    """
    Cleans and restructures a JSON response from Yahoo Finance.
    This function takes a JSON response from Yahoo Finance and restructures it by renaming keys and organizing the data into a more readable format. It handles various metadata fields and indicators, ensuring that the data is properly nested and renamed for easier access.
    Args:
        JSONResponse (dict): The original JSON response from Yahoo Finance.
    Returns:
        dict: The cleaned and restructured JSON response.
    """

    JSONResponse["Metadata"] = (
        JSONResponse.pop("meta") if "meta" in JSONResponse else None
    )
    JSONResponse["Timestamp"] = (
        JSONResponse.pop("timestamp") if "timestamp" in JSONResponse else None
    )
    JSONResponse["Indicators"] = (
        JSONResponse.pop("indicators") if "indicators" in JSONResponse else None
    )

    JSONResponse["Metadata"]["Chart Previous Close"] = (
        JSONResponse["Metadata"].pop("chartPreviousClose")
        if "chartPreviousClose" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Currency"] = (
        JSONResponse["Metadata"].pop("currency")
        if "currency" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"] = (
        JSONResponse["Metadata"].pop("currentTradingPeriod")
        if "currentTradingPeriod" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Granularity"] = (
        JSONResponse["Metadata"].pop("dataGranularity")
        if "dataGranularity" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Exchange Name"] = (
        JSONResponse["Metadata"].pop("exchangeName")
        if "exchangeName" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Exchange Timezone Name"] = (
        JSONResponse["Metadata"].pop("exchangeTimezoneName")
        if "exchangeTimezoneName" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Fifty Two Week High"] = (
        JSONResponse["Metadata"].pop("fiftyTwoWeekHigh")
        if "fiftyTwoWeekHigh" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Fifty Two Week Low"] = (
        JSONResponse["Metadata"].pop("fiftyTwoWeekLow")
        if "fiftyTwoWeekLow" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["First Trade Date"] = (
        JSONResponse["Metadata"].pop("firstTradeDate")
        if "firstTradeDate" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Full Exchange Name"] = (
        JSONResponse["Metadata"].pop("fullExchangeName")
        if "fullExchangeName" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["GMT Offset"] = (
        JSONResponse["Metadata"].pop("gmtoffset")
        if "gmtoffset" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Pre/Post Market"] = (
        JSONResponse["Metadata"].pop("hasPrePostMarketData")
        if "hasPrePostMarketData" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Instrument Type"] = (
        JSONResponse["Metadata"].pop("instrumentType")
        if "instrumentType" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Long Name"] = (
        JSONResponse["Metadata"].pop("longName")
        if "longName" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Previous Close"] = (
        JSONResponse["Metadata"].pop("previousClose")
        if "previousClose" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Price Hint"] = (
        JSONResponse["Metadata"].pop("priceHint")
        if "priceHint" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Range"] = (
        JSONResponse["Metadata"].pop("range")
        if "range" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Regular Market High"] = (
        JSONResponse["Metadata"].pop("regularMarketDayHigh")
        if "regularMarketDayHigh" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Regular Market Low"] = (
        JSONResponse["Metadata"].pop("regularMarketDayLow")
        if "regularMarketDayLow" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Regular Market Price"] = (
        JSONResponse["Metadata"].pop("regularMarketPrice")
        if "regularMarketPrice" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Regular Market Time"] = (
        JSONResponse["Metadata"].pop("regularMarketTime")
        if "regularMarketTime" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Regular Market Volume"] = (
        JSONResponse["Metadata"].pop("regularMarketVolume")
        if "regularMarketVolume" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Scale"] = (
        JSONResponse["Metadata"].pop("scale")
        if "scale" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Short Name"] = (
        JSONResponse["Metadata"].pop("shortName")
        if "shortName" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Ticker"] = (
        JSONResponse["Metadata"].pop("symbol")
        if "symbol" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Timezone"] = (
        JSONResponse["Metadata"].pop("timezone")
        if "timezone" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Trading Periods"] = (
        JSONResponse["Metadata"].pop("tradingPeriods")
        if "tradingPeriods" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Valid Ranges"] = (
        JSONResponse["Metadata"].pop("validRanges")
        if "validRanges" in JSONResponse["Metadata"]
        else None
    )

    CurrentTradingPeriod = (
        JSONResponse["Metadata"]["Current Trading Period"]
        if "Current Trading Period" in JSONResponse["Metadata"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Pre"] = (
        JSONResponse["Metadata"]["Current Trading Period"].pop("pre")
        if "pre" in JSONResponse["Metadata"]["Current Trading Period"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Regular"] = (
        JSONResponse["Metadata"]["Current Trading Period"].pop("regular")
        if "regular" in JSONResponse["Metadata"]["Current Trading Period"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Post"] = (
        JSONResponse["Metadata"]["Current Trading Period"].pop("post")
        if "post" in JSONResponse["Metadata"]["Current Trading Period"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Pre"]["Timezone"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Pre"].pop(
            "timezone"
        )
        if "timezone"
        in JSONResponse["Metadata"]["Current Trading Period"]["Pre"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Pre"]["Start"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Pre"].pop("start")
        if "start" in JSONResponse["Metadata"]["Current Trading Period"]["Pre"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Pre"]["End"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Pre"].pop("end")
        if "end" in JSONResponse["Metadata"]["Current Trading Period"]["Pre"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Pre"]["GMT Offset"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Pre"].pop(
            "gmtoffset"
        )
        if "gmtoffset"
        in JSONResponse["Metadata"]["Current Trading Period"]["Pre"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Regular"][
        "Timezone"
    ] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Regular"].pop(
            "timezone"
        )
        if "timezone"
        in JSONResponse["Metadata"]["Current Trading Period"]["Regular"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Regular"]["Start"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Regular"].pop(
            "start"
        )
        if "start"
        in JSONResponse["Metadata"]["Current Trading Period"]["Regular"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Regular"]["End"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Regular"].pop("end")
        if "end"
        in JSONResponse["Metadata"]["Current Trading Period"]["Regular"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Regular"][
        "GMT Offset"
    ] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Regular"].pop(
            "gmtoffset"
        )
        if "gmtoffset"
        in JSONResponse["Metadata"]["Current Trading Period"]["Regular"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Post"]["Timezone"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Post"].pop(
            "timezone"
        )
        if "timezone"
        in JSONResponse["Metadata"]["Current Trading Period"]["Post"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Post"]["Start"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Post"].pop("start")
        if "start" in JSONResponse["Metadata"]["Current Trading Period"]["Post"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Post"]["End"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Post"].pop("end")
        if "end" in JSONResponse["Metadata"]["Current Trading Period"]["Post"]
        else None
    )
    JSONResponse["Metadata"]["Current Trading Period"]["Post"]["GMT Offset"] = (
        JSONResponse["Metadata"]["Current Trading Period"]["Post"].pop(
            "gmtoffset"
        )
        if "gmtoffset"
        in JSONResponse["Metadata"]["Current Trading Period"]["Post"]
        else None
    )

    if "Metadata" in JSONResponse:
        if JSONResponse["Metadata"] != None:
            if "Trading Periods" in JSONResponse["Metadata"]:
                if JSONResponse["Metadata"]["Trading Periods"] != None:
                    for i in range(
                        len(JSONResponse["Metadata"]["Trading Periods"])
                    ):
                        JSONResponse["Metadata"]["Trading Periods"][i][0][
                            "Timezone"
                        ] = (
                            JSONResponse["Metadata"]["Trading Periods"][i][
                                0
                            ].pop("timezone")
                            if "timezone"
                            in JSONResponse["Metadata"]["Trading Periods"][i][0]
                            else None
                        )
                        JSONResponse["Metadata"]["Trading Periods"][i][0][
                            "Start"
                        ] = (
                            JSONResponse["Metadata"]["Trading Periods"][i][
                                0
                            ].pop("start")
                            if "start"
                            in JSONResponse["Metadata"]["Trading Periods"][i][0]
                            else None
                        )
                        JSONResponse["Metadata"]["Trading Periods"][i][0][
                            "End"
                        ] = (
                            JSONResponse["Metadata"]["Trading Periods"][i][
                                0
                            ].pop("end")
                            if "end"
                            in JSONResponse["Metadata"]["Trading Periods"][i][0]
                            else None
                        )
                        JSONResponse["Metadata"]["Trading Periods"][i][0][
                            "GMT Offset"
                        ] = (
                            JSONResponse["Metadata"]["Trading Periods"][i][
                                0
                            ].pop("gmtoffset")
                            if "gmtoffset"
                            in JSONResponse["Metadata"]["Trading Periods"][i][0]
                            else None
                        )

    if "Indicators" in JSONResponse:
        if JSONResponse["Indicators"] != None:
            if "quote" in JSONResponse["Indicators"]:
                if JSONResponse["Indicators"]["quote"] != None:
                    JSONResponse["Indicators"]["Quote"] = (
                        JSONResponse["Indicators"].pop("quote")
                        if "quote" in JSONResponse["Indicators"]
                        else None
                    )
                    for i in range(len(JSONResponse["Indicators"]["Quote"])):
                        JSONResponse["Indicators"]["Quote"][i]["Open"] = (
                            JSONResponse["Indicators"]["Quote"][i].pop("open")
                            if "open" in JSONResponse["Indicators"]["Quote"][i]
                            else None
                        )
                        JSONResponse["Indicators"]["Quote"][i]["High"] = (
                            JSONResponse["Indicators"]["Quote"][i].pop("high")
                            if "high" in JSONResponse["Indicators"]["Quote"][i]
                            else None
                        )
                        JSONResponse["Indicators"]["Quote"][i]["Low"] = (
                            JSONResponse["Indicators"]["Quote"][i].pop("low")
                            if "low" in JSONResponse["Indicators"]["Quote"][i]
                            else None
                        )
                        JSONResponse["Indicators"]["Quote"][i]["Close"] = (
                            JSONResponse["Indicators"]["Quote"][i].pop("close")
                            if "close" in JSONResponse["Indicators"]["Quote"][i]
                            else None
                        )
                        JSONResponse["Indicators"]["Quote"][i]["Volume"] = (
                            JSONResponse["Indicators"]["Quote"][i].pop("volume")
                            if "volume"
                            in JSONResponse["Indicators"]["Quote"][i]
                            else None
                        )

            if "adjclose" in JSONResponse["Indicators"]:
                if JSONResponse["Indicators"]["adjclose"] != None:
                    JSONResponse["Indicators"]["AdjClose"] = (
                        JSONResponse["Indicators"].pop("adjclose")
                        if "adjclose" in JSONResponse["Indicators"]
                        else None
                    )
                    for i in range(len(JSONResponse["Indicators"]["AdjClose"])):
                        JSONResponse["Indicators"]["Quote"][i][
                            "Adjusted Close"
                        ] = (
                            JSONResponse["Indicators"]["AdjClose"][i].pop(
                                "adjclose"
                            )
                            if "adjclose"
                            in JSONResponse["Indicators"]["AdjClose"][i]
                            else None
                        )
                        JSONResponse["Indicators"].pop("AdjClose")

    return JSONResponse


def CleanedResponseToDataFrame(CleanJSONResponse):
    """
    Converts a cleaned JSON response from Yahoo Finance into a pandas DataFrame.
    Args:
        CleanJSONResponse (dict): A dictionary containing the cleaned JSON response from Yahoo Finance.
            The dictionary is expected to have the following structure:
            {
                "Metadata": {
                    "Ticker": str
                },
                "Timestamp": list of int,
                "Indicators": {
                    "Quote": [
                        {
                            "Open": list of float,
                            "High": list of float,
                            "Low": list of float,
                            "Adjusted Close": list of float,
                            "Close": list of float,
                            "Volume": list of int
                        }
                }
            }
    Returns:
        pandas.DataFrame: A DataFrame with the following columns:
            - Date (as index)
            - Open
            - High
            - Low
            - Adjusted Close
            - Close
            - Volume
        The columns are multi-indexed with the ticker symbol as the first level.
    """

    DataFrame = pandas.DataFrame()
    Ticker = CleanJSONResponse["Metadata"]["Ticker"]
    DataFrame["Date"] = [
        datetime.datetime.fromtimestamp(ts)
        for ts in CleanJSONResponse["Timestamp"]
    ]
    DataFrame["Open"] = CleanJSONResponse["Indicators"]["Quote"][0]["Open"]
    DataFrame["High"] = CleanJSONResponse["Indicators"]["Quote"][0]["High"]
    DataFrame["Low"] = CleanJSONResponse["Indicators"]["Quote"][0]["Low"]
    DataFrame["Adjusted Close"] = CleanJSONResponse["Indicators"]["Quote"][0][
        "Adjusted Close"
    ]
    DataFrame["Close"] = CleanJSONResponse["Indicators"]["Quote"][0]["Close"]
    DataFrame["Volume"] = CleanJSONResponse["Indicators"]["Quote"][0]["Volume"]
    DataFrame.set_index("Date", inplace=True)
    DataFrame.columns = pandas.MultiIndex.from_product(
        [[Ticker], DataFrame.columns]
    )
    return DataFrame


def GranularityToDBGranularity(Granularity):
    """
    Convert a given granularity string to its corresponding database granularity string.
    Args:
        Granularity (str): The granularity string to be converted.
                           Possible values include "1m", "2m", "5m", "15m", "30m", "60m",
                           "90m", "1h", "1d", "1wk", "1mo", "3mo", "6mo", "1y".
    Returns:
        str: The corresponding database granularity string.
             Returns "Unknown" if the input granularity is not recognized.
    """

    GranularityMapping = {
        "1m": "Minute",
        "2m": "2-Minute",
        "5m": "5-Minute",
        "15m": "15-Minute",
        "30m": "30-Minute",
        "60m": "1h",
        "90m": "90-Minute",
        "1h": "Hourly",
        "1d": "Daily",
        "1wk": "Weekly",
        "1mo": "Monthly",
        "3mo": "3-Monthly",
        "6mo": "6-Monthly",
        "1y": "Yearly",
    }
    return GranularityMapping.get(Granularity, "Unknown")


def ReadyToStoreData(CleanedJSONResponse):
    """
    Processes the cleaned JSON response and prepares a dictionary with relevant data for storage.
    Args:
        CleanedJSONResponse (dict): The cleaned JSON response containing metadata, timestamps, and indicators.
    Returns:
        dict: A dictionary containing the processed data, including ticker metadata, request timestamps,
              response timestamps, events (dividends and stock splits), and indicators (open, high, low,
              close, volume, adjusted close).
    """

    Ticker = (
        CleanedJSONResponse["Metadata"]["Ticker"]
        if "Ticker" in CleanedJSONResponse["Metadata"]
        else None
    )

    Granularity = (
        CleanedJSONResponse["Metadata"]["Granularity"]
        if "Granularity" in CleanedJSONResponse["Metadata"]
        else None
    )

    DB_Granularity = (
        GranularityToDBGranularity(Granularity) if Granularity else None
    )

    Data = {}

    Data["TickerMetadatas"] = (
        CleanedJSONResponse["Metadata"]
        if "Metadata" in CleanedJSONResponse
        else None
    )

    Data["Request_StartTimestamp"] = (
        CleanedJSONResponse["Request_StartTimestamp"]
        if "Request_StartTimestamp" in CleanedJSONResponse
        else None
    )
    Data["Request_EndTimestamp"] = (
        CleanedJSONResponse["Request_EndTimestamp"]
        if "Request_EndTimestamp" in CleanedJSONResponse
        else None
    )

    Data["Request_Ticker"] = Ticker if Ticker else None
    Data["Request_Granularity"] = Granularity if Granularity else None
    Data["Request_Timestamp"] = (
        int(datetime.datetime.now().timestamp()) if Granularity else None
    )

    Timestamps = (
        CleanedJSONResponse["Timestamp"]
        if "Timestamp" in CleanedJSONResponse
        else None
    )

    Data["Timestamps"] = Timestamps if Timestamps else None
    Data["Response_Length"] = len(Timestamps) if Timestamps else 0
    Data["Response_Start_Timestamp"] = Timestamps[0] if Timestamps else None
    Data["Response_End_Timestamp"] = Timestamps[-1] if Timestamps else None

    if "events" in CleanedJSONResponse:
        if "dividends" in CleanedJSONResponse["events"]:
            Data["Dividends"] = CleanedJSONResponse["events"]["dividends"]
        if "splits" in CleanedJSONResponse["events"]:
            Data["Stock Splits"] = CleanedJSONResponse["events"]["splits"]

    Data["Open"] = (
        CleanedJSONResponse["Indicators"]["Quote"][0]["Open"]
        if "Open" in CleanedJSONResponse["Indicators"]["Quote"][0]
        else None
    )
    Data["High"] = (
        CleanedJSONResponse["Indicators"]["Quote"][0]["High"]
        if "High" in CleanedJSONResponse["Indicators"]["Quote"][0]
        else None
    )
    Data["Low"] = (
        CleanedJSONResponse["Indicators"]["Quote"][0]["Low"]
        if "Low" in CleanedJSONResponse["Indicators"]["Quote"][0]
        else None
    )
    Data["Close"] = (
        CleanedJSONResponse["Indicators"]["Quote"][0]["Close"]
        if "Close" in CleanedJSONResponse["Indicators"]["Quote"][0]
        else None
    )
    Data["Volume"] = (
        CleanedJSONResponse["Indicators"]["Quote"][0]["Volume"]
        if "Volume" in CleanedJSONResponse["Indicators"]["Quote"][0]
        else None
    )
    Data["AdjustedClose"] = (
        CleanedJSONResponse["Indicators"]["Quote"][0]["Adjusted Close"]
        if "Adjusted Close" in CleanedJSONResponse["Indicators"]["Quote"][0]
        else None
    )

    return Data


def StoreData(Data):
    """
    Stores the provided data into a MongoDB Collection.
    Args:
        Data (dict): A dictionary containing the data to be stored. It must include the following keys:
            - "Request_Ticker" (str): The ticker symbol of the data.
            - "Request_Granularity" (str): The granularity of the data.
            - "Timestamps" (list): A list of timestamps associated with the data.
    The function converts the granularity to a database-compatible format, connects to a MongoDB client,
    and inserts the data into a Collection named after the ticker and granularity. After insertion, the
    MongoDB client is closed. A message is printed to confirm the storage of the data, including the ticker,
    granularity, and the number of records stored.
    """

    Ticker = Data["Request_Ticker"]
    Granularity = Data["Request_Granularity"]
    Data["Timestamps"]

    DB_Granularity = GranularityToDBGranularity(Granularity)

    Client = pymongo.MongoClient()
    Database = Client["History"]
    Collection = Database[f"{Ticker}_{DB_Granularity}"]

    Collection.insert_one(Data)

    Client.close()

    print(
        "Stored Data: {Ticker} - {Granularity} - {Response_Length} records".format(
            Ticker=Ticker,
            Granularity=Granularity,
            Response_Length=len(Data["Timestamps"]),
        )
    )


def GetStoredData(Ticker, Granularity):
    """
    Retrieve stored data for a given ticker and granularity from a MongoDB Collection.
    Args:
        Ticker (str): The stock ticker symbol.
        Granularity (str): The granularity of the data (e.g., 'daily', 'weekly').
    Returns:
        dict: The stored data from the MongoDB Collection, or None if no data is found.
    """

    DB_Granularity = GranularityToDBGranularity(Granularity)

    Client = pymongo.MongoClient()
    Database = Client["History"]
    Collection = Database[f"{Ticker}_{DB_Granularity}"]

    Data = Collection.find_one()
    Client.close()
    return Data


def StoredDataToDataFrame(StoredData):
    """
    Converts stored stock data into a pandas DataFrame.
    Parameters:
    StoredData (dict): A dictionary containing stock data with the following keys:
        - "Timestamps" (list): List of timestamps.
        - "Open" (list): List of opening prices.
        - "High" (list): List of highest prices.
        - "Low" (list): List of lowest prices.
        - "Close" (list): List of closing prices.
        - "AdjustedClose" (list): List of adjusted closing prices.
        - "Volume" (list): List of trading volumes.
        - "Request_Ticker" (str): The ticker symbol of the stock.
    Returns:
    pandas.DataFrame: A DataFrame with the stock data, indexed by date and with a multi-level column index where the first level is the ticker symbol.
    """

    DataFrame = pandas.DataFrame()
    DataFrame["Date"] = [
        datetime.datetime.fromtimestamp(ts) for ts in StoredData["Timestamps"]
    ]
    DataFrame["Open"] = StoredData["Open"]
    DataFrame["High"] = StoredData["High"]
    DataFrame["Low"] = StoredData["Low"]
    DataFrame["Close"] = StoredData["Close"]
    DataFrame["Adjusted Close"] = StoredData["AdjustedClose"]
    DataFrame["Volume"] = StoredData["Volume"]
    DataFrame.set_index("Date", inplace=True)
    DataFrame.columns = pandas.MultiIndex.from_product(
        [[StoredData["Request_Ticker"]], DataFrame.columns]
    )

    return DataFrame


def IsQueriedDataToUpdate(Ticker, Granularity, StartDate, EndDate):
    """
    Determines if the queried data needs to be updated in the database.
    Args:
        Ticker (str): The ticker symbol of the financial instrument.
        Granularity (str): The granularity of the data (e.g., daily, hourly).
        StartDate (str): The start date of the queried data in 'YYYY-MM-DD' format.
        EndDate (str): The end date of the queried data in 'YYYY-MM-DD' format.
    Returns:
        dict: A dictionary containing the update information. The dictionary can have the following keys:
            - "CompleteUpdate": If the Collection for the ticker and granularity does not exist in the database.
            - "StartUpdate": If the queried start date is earlier than the stored start date.
            - "EndUpdate": If the queried end date is later than the stored end date.
            - An empty dictionary if no update is needed.
    """
    # Convert queried start and end dates to timestamps
    QueriedStartTS = int(pandas.Timestamp(StartDate).timestamp())
    QueriedEndTS = int(pandas.Timestamp(EndDate).timestamp())
    DB_Granularity = GranularityToDBGranularity(Granularity)

    # Connect to MongoDB client and get the list of collections
    Client = pymongo.MongoClient()
    Database = Client["History"]
    DataCollections = Database.list_collection_names()

    Update = {}

    # Check if the Collection for the ticker and granularity exists
    if f"{Ticker}_{DB_Granularity}" not in DataCollections:
        Update["CompleteUpdate"] = {
            "Ticker": Ticker,
            "Granularity": Granularity,
            "StartDate": QueriedStartTS,
            "EndDate": QueriedEndTS,
        }
        Client.close()
        return Update

    # Retrieve stored data for the ticker and granularity
    Collection = Database[f"{Ticker}_{DB_Granularity}"]
    Data = Collection.find_one()
    Client.close()

    # Get stored start and end timestamps and first trade date
    StoredStartDateTS = Data.get("Request_StartTimestamp")
    StoredEndDateTS = Data.get("Request_EndTimestamp")
    FirstTradeDateTS = Data.get("TickerMetadatas", {}).get("First Trade Date")

    # Check if the queried date range is within the stored date range
    if QueriedStartTS >= StoredStartDateTS and QueriedEndTS <= StoredEndDateTS:
        return {}

    # Determine if a start update is needed
    if QueriedStartTS < StoredStartDateTS:
        if FirstTradeDateTS and QueriedStartTS < FirstTradeDateTS:
            if QueriedEndTS <= FirstTradeDateTS:
                return {}
            else:
                Update["StartUpdate"] = {
                    "Ticker": Ticker,
                    "Granularity": Granularity,
                    "StartDate": QueriedStartTS,
                    "EndDate": StoredStartDateTS,
                }
        else:
            Update["StartUpdate"] = {
                "Ticker": Ticker,
                "Granularity": Granularity,
                "StartDate": QueriedStartTS,
                "EndDate": StoredStartDateTS,
            }

    # Determine if an end update is needed
    if QueriedEndTS > StoredEndDateTS:
        Update["EndUpdate"] = {
            "Ticker": Ticker,
            "Granularity": Granularity,
            "StartDate": StoredEndDateTS,
            "EndDate": QueriedEndTS,
        }

    return Update


def GetMissingData(ToUpdate):
    """
    Retrieves missing data for a given set of update instructions.
    Args:
        ToUpdate (dict): A dictionary containing update instructions. The keys can be:
            - "CompleteUpdate": A dictionary with keys "Ticker", "StartDate", "EndDate", and "Granularity".
            - "StartUpdate": A dictionary with keys "Ticker", "StartDate", "EndDate", and "Granularity".
            - "EndUpdate": A dictionary with keys "Ticker", "StartDate", "EndDate", and "Granularity".
    Returns:
        dict: A dictionary containing the updated data. The keys can be:
            - "Whole": Data for the complete update period.
            - "Before": Data for the start update period.
            - "After": Data for the end update period.
            Each of these keys maps to a dictionary with the cleaned JSON response and a boolean
            "MaxPastDateReached" indicating if the earliest date in the data is less than or equal
            to the requested start date.
    """
    UpdatedData = {}
    if not ToUpdate:
        return UpdatedData

    # Handle complete update
    if "CompleteUpdate" in ToUpdate:
        Complete = ToUpdate["CompleteUpdate"]
        CompleteStart = datetime.datetime.fromtimestamp(
            Complete["StartDate"]
        ).date()
        CompleteEnd = datetime.datetime.fromtimestamp(
            Complete["EndDate"]
        ).date()
        Resp = GetJSONResponse(
            Complete["Ticker"],
            Period=None,
            Granularity=Complete["Granularity"],
            StartDate=CompleteStart,
            EndDate=CompleteEnd,
            Logs=False,
        )
        UpdatedData["Whole"] = GetCleanedJSONResponse(Resp)
        Earliest = UpdatedData["Whole"]["Metadata"].get(
            "First Trade Date", None
        )
        if (
            Earliest
            and CompleteStart
            <= datetime.datetime.fromtimestamp(Earliest).date()
        ):
            UpdatedData["Whole"]["MaxPastDateReached"] = True
        else:
            UpdatedData["Whole"]["MaxPastDateReached"] = False

    # Handle start update
    if "StartUpdate" in ToUpdate:
        Start = ToUpdate["StartUpdate"]
        StartStart = datetime.datetime.fromtimestamp(Start["StartDate"]).date()
        StartEnd = datetime.datetime.fromtimestamp(Start["EndDate"]).date()
        Resp = GetJSONResponse(
            Start["Ticker"],
            Period=None,
            Granularity=Start["Granularity"],
            StartDate=StartStart,
            EndDate=StartEnd,
            Logs=False,
        )
        UpdatedData["Before"] = GetCleanedJSONResponse(Resp)
        Earliest = UpdatedData["Before"]["Metadata"].get(
            "First Trade Date", None
        )
        if (
            Earliest
            and StartEnd <= datetime.datetime.fromtimestamp(Earliest).date()
        ):
            UpdatedData["Before"]["MaxPastDateReached"] = True
        else:
            UpdatedData["Before"]["MaxPastDateReached"] = False

    # Handle end update
    if "EndUpdate" in ToUpdate:
        End = ToUpdate["EndUpdate"]
        EndStart = datetime.datetime.fromtimestamp(End["StartDate"]).date()
        EndEnd = datetime.datetime.fromtimestamp(End["EndDate"]).date()
        Resp = GetJSONResponse(
            End["Ticker"],
            Period=None,
            Granularity=End["Granularity"],
            StartDate=EndStart,
            EndDate=EndEnd,
            Logs=False,
        )
        UpdatedData["After"] = GetCleanedJSONResponse(Resp)
        UpdatedData["After"]["MaxPastDateReached"] = False

    return UpdatedData


def MergeInDataFrame(AggregatedData: dict, ReadyData: dict) -> dict:
    """
    Merges two sets of financial data into a single aggregated dataset.
    This function takes two dictionaries containing financial data, converts the timestamps to datetime objects,
    merges the data into a single DataFrame, removes duplicates, sorts by date, and updates the original dictionary
    with the merged data.
    Args:
        AggregatedData (dict): A dictionary containing the aggregated financial data with the following keys:
            - "Timestamps": List of timestamps.
            - "Open": List of opening prices.
            - "High": List of highest prices.
            - "Low": List of lowest prices.
            - "Close": List of closing prices.
            - "AdjustedClose": List of adjusted closing prices.
            - "Volume": List of trading volumes.
        ReadyData (dict): A dictionary containing the new financial data to be merged with the same structure as AggregatedData.
    Returns:
        dict: The updated AggregatedData dictionary with the merged and sorted financial data.
    """

    # Convert timestamps to datetime objects and create DataFrames for both aggregated and ready data
    FirstDataFrame = pandas.DataFrame(
        {
            "Date": [
                datetime.datetime.fromtimestamp(ts)
                for ts in AggregatedData["Timestamps"]
            ],
            "Open": AggregatedData["Open"],
            "High": AggregatedData["High"],
            "Low": AggregatedData["Low"],
            "Close": AggregatedData["Close"],
            "AdjustedClose": AggregatedData["AdjustedClose"],
            "Volume": AggregatedData["Volume"],
        }
    )

    SecondDataFrame = pandas.DataFrame(
        {
            "Date": [
                datetime.datetime.fromtimestamp(ts)
                for ts in ReadyData["Timestamps"]
            ],
            "Open": ReadyData["Open"],
            "High": ReadyData["High"],
            "Low": ReadyData["Low"],
            "Close": ReadyData["Close"],
            "AdjustedClose": ReadyData["AdjustedClose"],
            "Volume": ReadyData["Volume"],
        }
    )

    # Merge the two DataFrames, drop duplicates, and sort by date
    MergedDataFrame = pandas.concat(
        [FirstDataFrame, SecondDataFrame], ignore_index=True
    )
    MergedDataFrame.drop_duplicates(subset=["Date"], keep="last", inplace=True)
    MergedDataFrame.sort_values(by="Date", inplace=True)

    # Update the aggregated data with the merged DataFrame values
    new_timestamps = [int(dt.timestamp()) for dt in MergedDataFrame["Date"]]
    AggregatedData["Timestamps"] = new_timestamps
    AggregatedData["Open"] = MergedDataFrame["Open"].tolist()
    AggregatedData["High"] = MergedDataFrame["High"].tolist()
    AggregatedData["Low"] = MergedDataFrame["Low"].tolist()
    AggregatedData["Close"] = MergedDataFrame["Close"].tolist()
    AggregatedData["AdjustedClose"] = MergedDataFrame["AdjustedClose"].tolist()
    AggregatedData["Volume"] = MergedDataFrame["Volume"].tolist()

    # Update the response length
    AggregatedData["Response_Length"] = len(new_timestamps)

    return AggregatedData


def AggregateMissingAndExistingData(MissingData):
    """
    Aggregates missing and existing data from the provided MissingData dictionary.
    This function retrieves stored data from a MongoDB Collection based on the metadata in MissingData.
    If no stored data exists, it prepares and returns aggregated data.
    Otherwise, it merges missing data into the stored data, updates relevant timestamps, and returns the aggregated result.
    Args:
        MissingData (dict): A dictionary containing missing data keyed by period identifiers. Each key maps to a dictionary with metadata and data for the respective period.
    Returns:
        dict or None: The aggregated data dictionary if MissingData is not empty, otherwise None.
    """

    # If there is no missing data, return None as there'Start nothing to aggregate
    if not MissingData:
        return None

    # Extract keys from the MissingData dictionary
    Keys = list(MissingData.keys())
    Granularity = MissingData[Keys[0]]["Metadata"]["Granularity"]
    Ticker = MissingData[Keys[0]]["Metadata"]["Ticker"]

    # Convert granularity to database format and connect to MongoDB
    DB_Granularity = GranularityToDBGranularity(Granularity)
    Client = pymongo.MongoClient()
    Database = Client["History"]
    Collection = Database[f"{Ticker}_{DB_Granularity}"]
    StoredData = Collection.find_one()

    # If no stored data exists, prepare and return aggregated data
    if not StoredData:
        AggregatedData = ReadyToStoreData(
            MissingData.get("Whole")
            or MissingData.get("Before")
            or MissingData.get("After")
        )
        Client.close()
        return AggregatedData

    # Create a copy of the stored data to aggregate with new data
    AggregatedData = StoredData.copy()

    # Define common fields to retain in the aggregated data
    Common = [
        "Low",
        "Open",
        "High",
        "Close",
        "Volume",
        "Timestamps",
        "AdjustedClose",
        "Request_Ticker",
        "TickerMetadatas",
        "Response_Length",
        "Request_Timestamp",
        "Request_Granularity",
        "Request_EndTimestamp",
        "Request_StartTimestamp",
        "Response_End_Timestamp",
        "Response_Start_Timestamp",
    ]

    # Iterate over each period in MissingData to merge new data
    for PeriodKey, PeriodData in MissingData.items():
        if not PeriodData:
            continue
        # Prepare the new data for merging
        ReadyData = ReadyToStoreData(PeriodData)

        # Merge the new data into the aggregated data
        AggregatedData = MergeInDataFrame(AggregatedData, ReadyData)

        # Update the start timestamp if the period is a start update
        if "Before" in PeriodKey or "StartUpdate" in PeriodKey:
            AggregatedData["Request_StartTimestamp"] = min(
                AggregatedData["Request_StartTimestamp"],
                ReadyData["Request_StartTimestamp"],
            )
        # Update the end timestamp if the period is an end update or complete update
        if (
            "After" in PeriodKey
            or "EndUpdate" in PeriodKey
            or "Whole" in PeriodKey
        ):
            AggregatedData["Request_EndTimestamp"] = max(
                AggregatedData["Request_EndTimestamp"],
                ReadyData["Request_EndTimestamp"],
            )
        # Mark if the maximum past date has been reached
        if PeriodData.get("MaxPastDateReached", False):
            AggregatedData["MaxPastDateReached"] = True

    # Update the request timestamp to the current time
    AggregatedData["Request_Timestamp"] = int(
        datetime.datetime.now().timestamp()
    )
    Client.close()
    return AggregatedData


def StoreUpdatedData(AggregatedData):
    """
    Stores the updated aggregated data into the MongoDB database.
    This function connects to the MongoDB database, determines the appropriate
    Collection based on the ticker and granularity provided in the aggregated data,
    and either updates an existing Document or inserts a new one. It also prints a
    confirmation message indicating the number of records stored.
    Parameters:
        AggregatedData (dict): A dictionary containing aggregated data with keys:
            - "Request_Ticker": The ticker symbol.
            - "Request_Granularity": The data granularity.
            - "Timestamps" (optional): A list of timestamp records.
    Returns:
        None
    """
    # Extract ticker and granularity from the aggregated data
    Ticker = AggregatedData["Request_Ticker"]
    Granularity = AggregatedData["Request_Granularity"]
    # Convert granularity to database-compatible format
    DBGranularity = GranularityToDBGranularity(Granularity)

    # Initialize MongoDB client and select the appropriate database and Collection
    Client = pymongo.MongoClient()
    DataBase = Client["History"]
    Collection = DataBase[f"{Ticker}_{DBGranularity}"]

    # Retrieve the existing Document from the Collection
    Document = Collection.find_one()
    if Document:
        # If a Document exists, get its ID and update the aggregated data with this ID
        DocumentID = Document["_id"]
        AggregatedData["_id"] = DocumentID
        # Replace the existing Document with the updated aggregated data
        Collection.find_one_and_replace({"_id": DocumentID}, AggregatedData)
    else:
        # If no Document exists, insert the aggregated data as a new Document
        Collection.insert_one(AggregatedData)

    # Close the MongoDB client connection
    Client.close()

    # Count the number of timestamp records in the aggregated data
    TimestampsCount = (
        len(AggregatedData["Timestamps"])
        if "Timestamps" in AggregatedData and AggregatedData["Timestamps"]
        else 0
    )
    # Print a confirmation message with ticker, granularity, and record count
    print(
        f"Updated Data Stored: {Ticker} - {Granularity} - {TimestampsCount} records."
    )


def SingleTicker(Ticker=None, Granularity=None, StartDate=None, EndDate=None):
    """
    Retrieves and returns data for a single ticker based on the specified granularity and date range.
    Parameters:
        Ticker (str, optional): The ticker symbol to retrieve data for. Defaults to None.
        Granularity (str, optional): The granularity of the data (e.g., daily, weekly). Defaults to None.
        StartDate (str or datetime, optional): The start date for the data retrieval. Defaults to None.
        EndDate (str or datetime, optional): The end date for the data retrieval. Defaults to None.
    Returns:
        pandas.DataFrame: A DataFrame containing the ticker data between StartDate and EndDate.
    """
    # Determine if the queried data needs to be updated in the database
    ToUpdate = IsQueriedDataToUpdate(Ticker, Granularity, StartDate, EndDate)
    # Get the missing data based on the update requirements
    MissingData = GetMissingData(ToUpdate)

    if not MissingData:
        # If no missing data, retrieve stored data
        StoredData = GetStoredData(Ticker, Granularity)
        # Convert stored data to a DataFrame
        DataFrame = StoredDataToDataFrame(StoredData)
        # Ensure the DataFrame index is a DatetimeIndex
        if not isinstance(DataFrame.index, pandas.DatetimeIndex):
            DataFrame.index = pandas.to_datetime(DataFrame.index)
        # Sort the DataFrame by index
        DataFrame = DataFrame.sort_index()
        # Return the DataFrame within the specified date range
        return DataFrame[StartDate:EndDate]
    else:
        # Aggregate missing and existing data
        AggregatedData = AggregateMissingAndExistingData(MissingData)
        if AggregatedData is not None:
            # Store the updated aggregated data
            StoreUpdatedData(AggregatedData)
        # Retrieve the updated stored data
        StoredData = GetStoredData(Ticker, Granularity)
        # Convert stored data to a DataFrame
        DataFrame = StoredDataToDataFrame(StoredData)
        # Ensure the DataFrame index is a DatetimeIndex
        if not isinstance(DataFrame.index, pandas.DatetimeIndex):
            DataFrame.index = pandas.to_datetime(DataFrame.index)
        # Sort the DataFrame by index
        DataFrame = DataFrame.sort_index()
        # Return the DataFrame within the specified date range
        return DataFrame[StartDate:EndDate]


def MultipleTickers(
    Tickers=None, Granularity=None, StartDate=None, EndDate=None
):
    """
    Fetches and concatenates data for multiple tickers.

    Parameters:
        Tickers (list, optional): A list of ticker symbols to retrieve data for.
        Granularity (str, optional): The granularity of the data (e.g., 'daily', 'weekly').
        StartDate (str, optional): The start date for data retrieval in 'YYYY-MM-DD' format.
        EndDate (str, optional): The end date for data retrieval in 'YYYY-MM-DD' format.

    Returns:
        pandas.DataFrame: A DataFrame containing concatenated data for the specified tickers.
    """
    DataFrame = (
        pandas.DataFrame()
    )  # Initialize an empty DataFrame to store concatenated data
    for Tickr in Tickers:
        try:
            # Retrieve data for each ticker
            DataFrame2 = SingleTicker(Tickr, Granularity, StartDate, EndDate)
            # Concatenate the retrieved DataFrame with the main DataFrame along columns
            DataFrame = pandas.concat([DataFrame, DataFrame2], axis=1)
        except Exception as E:
            # Print error message if data retrieval fails for a ticker
            print(f"Error retrieving data for {Tickr}: {E}")
    return DataFrame  # Return the aggregated DataFrame containing data for all tickers


def HistoricalData(Ticker=None, Granularity=None, StartDate=None, EndDate=None):
    """
    Fetches historical data for specified ticker(Start) with given granularity and date range.
    Parameters:
        Ticker (str or list of str): The ticker symbol(Start) to retrieve data for.
        Granularity (str): The granularity of the data (e.g., '1m', '2m', '5m', '1h', '1d', '1wk', '1mo', ...).
        StartDate (str, optional): The start date for the historical data as 'YYYY-MM-DD'.
        EndDate (str, optional): The end date for the historical data as 'YYYY-MM-DD'.
    Returns:
        Historical data corresponding to the specified parameters.
    Raises:
        ValueError: If Ticker is not a string or a list of strings.
    """

    # Determine the type of Ticker and call the appropriate function
    if isinstance(Ticker, str):
        # If Ticker is a single string, fetch data for one ticker
        return SingleTicker(Ticker, Granularity, StartDate, EndDate)
    elif isinstance(Ticker, list):
        # If Ticker is a list, fetch data for multiple tickers
        return MultipleTickers(Ticker, Granularity, StartDate, EndDate)
    else:
        # Raise an error for invalid Ticker input types
        raise ValueError(
            "Invalid Ticker input. Provide a string or a list of strings."
        )
