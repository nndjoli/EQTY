"""
This module retrieves detailed financial and company information for a given ticker symbol
from Yahoo Finance, processes it to standardize key names, and stores it in a MongoDB database.
It ensures data freshness by updating information if it is older than 24 hours. Specific data points
such as balance sheets, earnings, and insider transactions can be fetched via dedicated functions.
"""

import datetime
import pymongo
import requests
import re

# Local module import for Yahoo credentials
from . import Credentials as YFCredentials

# Retrieve credentials for Yahoo Finance
Credentials = YFCredentials.Get()
Cookies = Credentials.Cookies
Crumb = Credentials.Crumb
Headers = Credentials.Headers

Options = [
    "assetProfile",
    "balanceSheetHistory",
    "balanceSheetHistoryQuarterly",
    "calendarEvents",
    "cashflowStatementHistory",
    "cashflowStatementHistoryQuarterly",
    "defaultKeyStatistics",
    "earnings",
    "earningsHistory",
    "earningsTrend",
    "esgScores",
    "financialData",
    "fundOwnership",
    "fundPerformance",
    "fundProfile",
    "indexTrend",
    "incomeStatementHistory",
    "incomeStatementHistoryQuarterly",
    "industryTrend",
    "insiderHolders",
    "insiderTransactions",
    "institutionOwnership",
    "majorHoldersBreakdown",
    "pageViews",
    "price",
    "quoteType",
    "recommendationTrend",
    "secFilings",
    "netSharePurchaseActivity",
    "sectorTrend",
    "summaryDetail",
    "summaryProfile",
    "topHoldings",
    "upgradeDowngradeHistory",
]


def RenameKey(Key):
    """
    Insert spaces before uppercase letters and convert to title case.
    Example: 'assetProfile' -> 'Asset Profile'.

    :param Key: The original key string.
    :return: The renamed key with spaces inserted and in title case.
    """
    SpacedKey = re.sub(r"([A-Z])", r" \1", Key)
    return SpacedKey.title()


def RenameKeysRecursively(Data):
    """
    Recursively rename keys in dictionaries (and dictionaries within lists).
    Uses the RenameKey function to insert spaces and title-case the key names.

    :param Data: The original dictionary or list to rename keys for.
    :return: A new dictionary or list with renamed keys.
    """
    if isinstance(Data, dict):
        NewDict = {}
        for OriginalKey, Value in Data.items():
            NewKey = RenameKey(OriginalKey)
            if isinstance(Value, dict):
                NewDict[NewKey] = RenameKeysRecursively(Value)
            elif isinstance(Value, list):
                NewList = []
                for Item in Value:
                    if isinstance(Item, dict):
                        NewList.append(RenameKeysRecursively(Item))
                    else:
                        NewList.append(Item)
                NewDict[NewKey] = NewList
            else:
                NewDict[NewKey] = Value
        return NewDict

    elif isinstance(Data, list):
        return [
            RenameKeysRecursively(Item) if isinstance(Item, dict) else Item
            for Item in Data
        ]
    else:
        return Data


def QuoteSummary(Ticker):
    """
    Fetch summary data from Yahoo Finance for a given ticker and rename keys.

    :param Ticker: The ticker symbol to retrieve data for.
    :return: A dictionary containing renamed data with 'Ticker' and 'Last Update'.
    """
    try:
        BaseURL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
        URL = (
            f"{BaseURL}{Ticker}?modules=assetprofile,calendarEvents,earningsHistory,"
            f"earningsTrend,earnings,esgScores,financialData,topHoldings,fundProfile,"
            f"fundOwnership,insiderHolders,insiderTransactions,institutionOwnership,"
            f"upgradeDowngradeHistory,indexTrend,industryTrend,defaultKeyStatistics,"
            f"majorHoldersBreakdown,pageViews,price,quoteType,quotes,recommendationTrend,"
            f"secFilings,netSharePurchaseActivity,summaryDetail,summaryProfile&formatted="
            f"false&lang=en-US&region=US&corsDomain=finance.yahoo.com&crumb={Crumb}"
        )

        Response = requests.get(URL, headers=Headers, cookies=Cookies)
        ResponseJSON = Response.json()["quoteSummary"]["result"][0]

        RenamedResponse = RenameKeysRecursively(ResponseJSON)
        RenamedResponse["Ticker"] = Ticker
        RenamedResponse["Last Update"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        return RenamedResponse

    except requests.exceptions.RequestException as Error:
        print(f"Request Error for {Ticker}: {Error}")
        return None
    except (KeyError, TypeError) as ParsingError:
        print(f"Parsing error for {Ticker}: {ParsingError}")
        return None


def StoreQuoteSummary(Response):
    """
    Store the given quote summary response in MongoDB.

    :param Response: The quote summary response dictionary to store.
    """

    Ticker = Response["Ticker"]
    Client = pymongo.MongoClient()
    DataBase = Client["Informations"]
    Collection = DataBase[Ticker]

    # Use replace_one with upsert=True to insert or update the document
    Collection.replace_one({"Ticker": Ticker}, Response, upsert=True)
    Client.close()


def FetchAndStore(Ticker):
    """
    Fetch up-to-date data from Yahoo Finance for the given ticker and store it in MongoDB.

    :param Ticker: The ticker symbol to fetch and store data for.
    :return: The newly fetched response, or None if an error occurred.
    """
    try:
        NewResponse = QuoteSummary(Ticker)
        if NewResponse is not None:
            StoreQuoteSummary(NewResponse)
            print(f"Stored new data for {Ticker} in MongoDB.")
        else:
            print(f"Could not fetch any data for {Ticker} from the API.")
        return NewResponse
    except Exception as E:
        print(f"Error fetching/storing data for {Ticker}: {E}")
        return None


def GetQuoteSummary(Ticker):
    """
    Retrieve quote summary data from MongoDB if it exists and is recent,
    otherwise fetch fresh data from Yahoo Finance and store it.

    :param Ticker: The ticker symbol to retrieve data for.
    :return: The quote summary dictionary.
    """
    try:
        Client = pymongo.MongoClient()
        DataBase = Client["Informations"]
        Collection = DataBase[Ticker]

        # Retrieve the first document in the collection
        Response = Collection.find_one()
        Client.close()

        # If no data in MongoDB, fetch from API
        if Response is None:
            print(
                f"No document found in MongoDB for {Ticker}. Fetching from API..."
            )
            return FetchAndStore(Ticker)
        else:
            LastUpdateString = Response.get("Last Update")
            if not LastUpdateString:
                print(
                    f"No 'Last Update' field found for {Ticker}, fetching from API..."
                )
                return FetchAndStore(Ticker)

            LastUpdateDatetime = datetime.datetime.strptime(
                LastUpdateString, "%Y-%m-%d %H:%M:%S"
            )
            NowDatetime = datetime.datetime.now()

            # If data is older than 24 hours, refresh it
            if (NowDatetime - LastUpdateDatetime) > datetime.timedelta(
                hours=24
            ):
                print(
                    f"Data for {Ticker} is older than 24h. Fetching from API..."
                )
                return FetchAndStore(Ticker)
            else:
                # Use existing data
                return Response

    except Exception as E:
        print(f"Error checking MongoDB for {Ticker}: {E}")
        return FetchAndStore(Ticker)


def CompleteInformations(Ticker):
    """
    Retrieve complete information for the given ticker.
    If data is outdated or missing, fetch a fresh copy.

    :param Ticker: The ticker symbol to retrieve data for.
    :return: A dictionary containing the complete information, or None on error.
    """
    try:
        Data = GetQuoteSummary(Ticker)
        return Data
    except Exception as E:
        print(f"Error fetching complete informations for {Ticker}: {E}")
        return None


def AssetProfile(Ticker):
    """
    Retrieve 'Asset Profile' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Asset Profile")
        if not SubData:
            raise KeyError("Asset Profile")
        return SubData
    except Exception as E:
        print(f"No Asset Profile information found for {Ticker}. Error: {E}")
        return None


def BalanceSheetHistory(Ticker):
    """
    Retrieve 'Balance Sheet History' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Balance Sheet History")
        if not SubData:
            raise KeyError("Balance Sheet History")
        return SubData
    except Exception as E:
        print(
            f"No Balance Sheet History information found for {Ticker}. Error: {E}"
        )
        return None


def BalanceSheetHistoryQuarterly(Ticker):
    """
    Retrieve 'Balance Sheet History Quarterly' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Balance Sheet History Quarterly")
        if not SubData:
            raise KeyError("Balance Sheet History Quarterly")
        return SubData
    except Exception as E:
        print(
            f"No Balance Sheet History Quarterly information found for {Ticker}. Error: {E}"
        )
        return None


def CalendarEvents(Ticker):
    """
    Retrieve 'Calendar Events' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Calendar Events")
        if not SubData:
            raise KeyError("Calendar Events")
        return SubData
    except Exception as E:
        print(f"No Calendar Events information found for {Ticker}. Error: {E}")
        return None


def CashflowStatementHistory(Ticker):
    """
    Retrieve 'Cashflow Statement History' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Cashflow Statement History")
        if not SubData:
            raise KeyError("Cashflow Statement History")
        return SubData
    except Exception as E:
        print(
            f"No Cashflow Statement History information found for {Ticker}. Error: {E}"
        )
        return None


def CashflowStatementHistoryQuarterly(Ticker):
    """
    Retrieve 'Cashflow Statement History Quarterly' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Cashflow Statement History Quarterly")
        if not SubData:
            raise KeyError("Cashflow Statement History Quarterly")
        return SubData
    except Exception as E:
        print(
            f"No Cashflow Statement History Quarterly information found for {Ticker}. Error: {E}"
        )
        return None


def DefaultKeyStatistics(Ticker):
    """
    Retrieve 'Default Key Statistics' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Default Key Statistics")
        if not SubData:
            raise KeyError("Default Key Statistics")
        return SubData
    except Exception as E:
        print(
            f"No Default Key Statistics information found for {Ticker}. Error: {E}"
        )
        return None


def Earnings(Ticker):
    """
    Retrieve 'Earnings' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Earnings")
        if not SubData:
            raise KeyError("Earnings")
        return SubData
    except Exception as E:
        print(f"No Earnings information found for {Ticker}. Error: {E}")
        return None


def EarningsHistory(Ticker):
    """
    Retrieve 'Earnings History' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Earnings History")
        if not SubData:
            raise KeyError("Earnings History")
        return SubData
    except Exception as E:
        print(f"No Earnings History information found for {Ticker}. Error: {E}")
        return None


def EarningsTrend(Ticker):
    """
    Retrieve 'Earnings Trend' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Earnings Trend")
        if not SubData:
            raise KeyError("Earnings Trend")
        return SubData
    except Exception as E:
        print(f"No Earnings Trend information found for {Ticker}. Error: {E}")
        return None


def EsgScores(Ticker):
    """
    Retrieve 'Esg Scores' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Esg Scores")
        if not SubData:
            raise KeyError("Esg Scores")
        return SubData
    except Exception as E:
        print(f"No Esg Scores information found for {Ticker}. Error: {E}")
        return None


def FinancialData(Ticker):
    """
    Retrieve 'Financial Data' for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Financial Data")
        if not SubData:
            raise KeyError("Financial Data")
        return SubData
    except Exception as E:
        print(f"No Financial Data information found for {Ticker}. Error: {E}")
        return None


def FundOwnership(Ticker):
    """
    Retrieve 'Fund Ownership' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Fund Ownership")
        if not SubData:
            raise KeyError("Fund Ownership")
        return SubData
    except Exception as E:
        print(f"No Fund Ownership information found for {Ticker}. Error: {E}")
        return None


def FundPerformance(Ticker):
    """
    Retrieve 'Fund Performance' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Fund Performance")
        if not SubData:
            raise KeyError("Fund Performance")
        return SubData
    except Exception as E:
        print(f"No Fund Performance information found for {Ticker}. Error: {E}")
        return None


def FundProfile(Ticker):
    """
    Retrieve 'Fund Profile' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Fund Profile")
        if not SubData:
            raise KeyError("Fund Profile")
        return SubData
    except Exception as E:
        print(f"No Fund Profile information found for {Ticker}. Error: {E}")
        return None


def IncomeStatementHistory(Ticker):
    """
    Retrieve 'Income Statement History' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Income Statement History")
        if not SubData:
            raise KeyError("Income Statement History")
        return SubData
    except Exception as E:
        print(
            f"No Income Statement History information found for {Ticker}. Error: {E}"
        )
        return None


def IncomeStatementHistoryQuarterly(Ticker):
    """
    Retrieve 'Income Statement History Quarterly' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Income Statement History Quarterly")
        if not SubData:
            raise KeyError("Income Statement History Quarterly")
        return SubData
    except Exception as E:
        print(
            f"No Income Statement History Quarterly information found for {Ticker}. Error: {E}"
        )
        return None


def IndexTrend(Ticker):
    """
    Retrieve 'Index Trend' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Index Trend")
        if not SubData:
            raise KeyError("Index Trend")
        return SubData
    except Exception as E:
        print(f"No Index Trend information found for {Ticker}. Error: {E}")
        return None


def IndustryTrend(Ticker):
    """
    Retrieve 'Industry Trend' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Industry Trend")
        if not SubData:
            raise KeyError("Industry Trend")
        return SubData
    except Exception as E:
        print(f"No Industry Trend information found for {Ticker}. Error: {E}")
        return None


def InsiderHolders(Ticker):
    """
    Retrieve 'Insider Holders' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Insider Holders")
        if not SubData:
            raise KeyError("Insider Holders")
        return SubData
    except Exception as E:
        print(f"No Insider Holders information found for {Ticker}. Error: {E}")
        return None


def InsiderTransactions(Ticker):
    """
    Retrieve 'Insider Transactions' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Insider Transactions")
        if not SubData:
            raise KeyError("Insider Transactions")
        return SubData
    except Exception as E:
        print(
            f"No Insider Transactions information found for {Ticker}. Error: {E}"
        )
        return None


def InstitutionOwnership(Ticker):
    """
    Retrieve 'Institution Ownership' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Institution Ownership")
        if not SubData:
            raise KeyError("Institution Ownership")
        return SubData
    except Exception as E:
        print(
            f"No Institution Ownership information found for {Ticker}. Error: {E}"
        )
        return None


def MajorHoldersBreakdown(Ticker):
    """
    Retrieve 'Major Holders Breakdown' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Major Holders Breakdown")
        if not SubData:
            raise KeyError("Major Holders Breakdown")
        return SubData
    except Exception as E:
        print(
            f"No Major Holders Breakdown information found for {Ticker}. Error: {E}"
        )
        return None


def NetSharePurchaseActivity(Ticker):
    """
    Retrieve 'Net Share Purchase Activity' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Net Share Purchase Activity")
        if not SubData:
            raise KeyError("Net Share Purchase Activity")
        return SubData
    except Exception as E:
        print(
            f"No Net Share Purchase Activity information found for {Ticker}. Error: {E}"
        )
        return None


def PageViews(Ticker):
    """
    Retrieve 'Page Views' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Page Views")
        if not SubData:
            raise KeyError("Page Views")
        return SubData
    except Exception as E:
        print(f"No Page Views information found for {Ticker}. Error: {E}")
        return None


def Price(Ticker):
    """
    Retrieve 'Price' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Price")
        if not SubData:
            raise KeyError("Price")
        return SubData
    except Exception as E:
        print(f"No Price information found for {Ticker}. Error: {E}")
        return None


def QuoteType(Ticker):
    """
    Retrieve 'Quote Type' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Quote Type")
        if not SubData:
            raise KeyError("Quote Type")
        return SubData
    except Exception as E:
        print(f"No Quote Type information found for {Ticker}. Error: {E}")
        return None


def RecommendationTrend(Ticker):
    """
    Retrieve 'Recommendation Trend' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Recommendation Trend")
        if not SubData:
            raise KeyError("Recommendation Trend")
        return SubData
    except Exception as E:
        print(
            f"No Recommendation Trend information found for {Ticker}. Error: {E}"
        )
        return None


def SecFilings(Ticker):
    """
    Retrieve 'Sec Filings' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Sec Filings")
        if not SubData:
            raise KeyError("Sec Filings")
        return SubData
    except Exception as E:
        print(f"No Sec Filings information found for {Ticker}. Error: {E}")
        return None


def SectorTrend(Ticker):
    """
    Retrieve 'Sector Trend' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Sector Trend")
        if not SubData:
            raise KeyError("Sector Trend")
        return SubData
    except Exception as E:
        print(f"No Sector Trend information found for {Ticker}. Error: {E}")
        return None


def SummaryDetail(Ticker):
    """
    Retrieve 'Summary Detail' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Summary Detail")
        if not SubData:
            raise KeyError("Summary Detail")
        return SubData
    except Exception as E:
        print(f"No Summary Detail information found for {Ticker}. Error: {E}")
        return None


def SummaryProfile(Ticker):
    """
    Retrieve 'Summary Profile' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Summary Profile")
        if not SubData:
            raise KeyError("Summary Profile")
        return SubData
    except Exception as E:
        print(f"No Summary Profile information found for {Ticker}. Error: {E}")
        return None


def TopHoldings(Ticker):
    """
    Retrieve 'Top Holdings' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Top Holdings")
        if not SubData:
            raise KeyError("Top Holdings")
        return SubData
    except Exception as E:
        print(f"No Top Holdings information found for {Ticker}. Error: {E}")
        return None


def UpgradeDowngradeHistory(Ticker):
    """
    Retrieve 'Upgrade Downgrade History' data for the given ticker.
    """
    Data = GetQuoteSummary(Ticker)
    try:
        SubData = Data.get("Upgrade Downgrade History")
        if not SubData:
            raise KeyError("Upgrade Downgrade History")
        return SubData
    except Exception as E:
        print(
            f"No Upgrade Downgrade History information found for {Ticker}. Error: {E}"
        )
        return None
