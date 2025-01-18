from yahooquery import Ticker
from math import ceil
from tqdm import tqdm
import pandas
import time


def FetchTickersInfo(TickersList, MaxRetries=3, SleepTime=10):

    Attempts = 0
    while Attempts < MaxRetries:
        try:
            # Create a multi-ticker query object and retrieve the 'quote_type' information.
            MultiTicker = Ticker(TickersList).quotes

            return MultiTicker

        except Exception as e:
            print(e)
            Attempts += 1
            print(
                f"Error processing batch (e.g., {TickersList[:3]}...), "
                f"Attempt {Attempts}/{MaxRetries}: {e}"
            )
            time.sleep(SleepTime)  # Wait before retrying in case of an error

    # If all attempts fail, return an empty dictionary.
    return {}


def BatchProcessTickers(DataFrame, BatchSize=500, WaitTime=10):
    # Get the list of unique ticker symbols from the DataFrame.
    UniqueTickers = DataFrame["Ticker"].unique().tolist()
    TotalUniqueTickers = len(UniqueTickers)

    # Calculate the number of batches needed to process all tickers.
    NumberOfBatches = ceil(TotalUniqueTickers / BatchSize)

    # Create a list of batches from the unique tickers.
    Batches = [
        UniqueTickers[i * BatchSize : (i + 1) * BatchSize]
        for i in range(NumberOfBatches)
    ]

    Results = {}  # Dictionary to store exchange results for all tickers.

    # Process each batch using a progress bar.
    for i, Batch in enumerate(
        tqdm(
            Batches,
            desc="Processing Batches".ljust(20),
            unit="Batch",
            ncols=100,  # Total width of the loadbar
            bar_format="{l_bar:20}{bar:30}{r_bar:20}",  # Display a 30 characters load bar
        )
    ):
        BatchResult = FetchTickersInfo(Batch, MaxRetries=3, SleepTime=WaitTime)
        Results.update(BatchResult)  # Merge the batch results into the overall results.

        # Wait between batches if not processing the last batch.
        if i < NumberOfBatches - 1:
            time.sleep(WaitTime)

    # Convert the dictionary of results into a DataFrame.
    InformationsDataFrame = pandas.DataFrame.from_dict(Results, orient="index")
    # Reset the index to bring the ticker symbol into a column.
    InformationsDataFrame = (
        pandas.DataFrame.from_dict(Results, orient="index")
        .reset_index()
        .rename(columns={"index": "Ticker"})
    )

    # Merge the original DataFrame with the info_df on the 'Ticker' column.
    FinalDataFrame = DataFrame.merge(InformationsDataFrame, on="Ticker", how="left")
    FinalDataFrame = FinalDataFrame.rename(
        columns={
            "Ticker": "Ticker",
            "Company Name": "Company Name",
            "Sector": "Sector",
            "Industry": "Industry",
            "Country": "Country",
            "currency_x": "CurrencyX",
            "Exchange Name": "Exchange Name",
            "language": "Language",
            "region": "Region",
            "quoteType": "Quote Type",
            "typeDisp": "Display Type",
            "quoteSourceName": "Quote Source Name",
            "triggerable": "Triggerable",
            "customPriceAlertConfidence": "Custom Price Alert Confidence",
            "currency_y": "CurrencyY",
            "currency": "Currency",
            "shortName": "Short Name",
            "longName": "Long Name",
            "corporateActions": "Corporate Actions",
            "regularMarketTime": "Regular Market Time",
            "marketState": "Market State",
            "exchange": "Exchange",
            "messageBoardId": "Message Board ID",
            "exchangeTimezoneName": "Exchange Timezone Name",
            "exchangeTimezoneShortName": "Exchange Timezone Short Name",
            "gmtOffSetMilliseconds": "GMT Offset Milliseconds",
            "market": "Market",
            "esgPopulated": "ESG Populated",
            "regularMarketChangePercent": "Regular Market Change Percent",
            "regularMarketPrice": "Regular Market Price",
            "hasPrePostMarketData": "Has Pre Post Market Data",
            "firstTradeDateMilliseconds": "First Trade Date Milliseconds",
            "priceHint": "Price Hint",
            "regularMarketChange": "Regular Market Change",
            "regularMarketDayHigh": "Regular Market Day High",
            "regularMarketDayRange": "Regular Market Day Range",
            "regularMarketDayLow": "Regular Market Day Low",
            "regularMarketVolume": "Regular Market Volume",
            "regularMarketPreviousClose": "Regular Market Previous Close",
            "bid": "Bid",
            "ask": "Ask",
            "bidSize": "Bid Size",
            "askSize": "Ask Size",
            "fullExchangeName": "Full Exchange Name",
            "financialCurrency": "Financial Currency",
            "regularMarketOpen": "Regular Market Open",
            "averageDailyVolume3Month": "Average Daily Volume 3 Month",
            "averageDailyVolume10Day": "Average Daily Volume 10 Day",
            "fiftyTwoWeekLowChange": "Fifty Two Week Low Change",
            "fiftyTwoWeekLowChangePercent": "Fifty Two Week Low Change Percent",
            "fiftyTwoWeekRange": "Fifty Two Week Range",
            "fiftyTwoWeekHighChange": "Fifty Two Week High Change",
            "fiftyTwoWeekHighChangePercent": "Fifty Two Week High Change Percent",
            "fiftyTwoWeekLow": "Fifty Two Week Low",
            "fiftyTwoWeekHigh": "Fifty Two Week High",
            "fiftyTwoWeekChangePercent": "Fifty Two Week Change Percent",
            "earningsTimestamp": "Earnings Timestamp",
            "earningsTimestampStart": "Earnings Timestamp Start",
            "earningsTimestampEnd": "Earnings Timestamp End",
            "earningsCallTimestampStart": "Earnings Call Timestamp Start",
            "earningsCallTimestampEnd": "Earnings Call Timestamp End",
            "isEarningsDateEstimate": "Is Earnings Date Estimate",
            "trailingAnnualDividendRate": "Trailing Annual Dividend Rate",
            "trailingPE": "Trailing PE",
            "dividendRate": "Dividend Rate",
            "trailingAnnualDividendYield": "Trailing Annual Dividend Yield",
            "dividendYield": "Dividend Yield",
            "epsTrailingTwelveMonths": "EPS Trailing Twelve Months",
            "epsForward": "EPS Forward",
            "epsCurrentYear": "EPS Current Year",
            "priceEpsCurrentYear": "Price EPS Current Year",
            "sharesOutstanding": "Shares Outstanding",
            "bookValue": "Book Value",
            "fiftyDayAverage": "Fifty Day Average",
            "fiftyDayAverageChange": "Fifty Day Average Change",
            "fiftyDayAverageChangePercent": "Fifty Day Average Change Percent",
            "twoHundredDayAverage": "Two Hundred Day Average",
            "twoHundredDayAverageChange": "Two Hundred Day Average Change",
            "twoHundredDayAverageChangePercent": "Two Hundred Day Average Change Percent",
            "marketCap": "Market Capitalization",  # Keep this one
            "forwardPE": "Forward PE",
            "priceToBook": "Price To Book",
            "sourceInterval": "Source Interval",
            "exchangeDataDelayedBy": "Exchange Data Delayed By",
            "averageAnalystRating": "Average Analyst Rating",
            "tradeable": "Tradeable",
            "cryptoTradeable": "Crypto Tradeable",
            "prevName": "Previous Name",
            "nameChangeDate": "Name Change Date",
            "ipoExpectedDate": "IPO Expected Date",
            "openInterest": "Open Interest",
            "newSymbol": "New Symbol",
            "dividendDate": "Dividend Date",
            "preMarketTime": "Pre Market Time",
            "preMarketChange": "Pre Market Change",
            "preMarketChangePercent": "Pre Market Change Percent",
            "preMarketPrice": "Pre Market Price",
            "displayName": "Display Name",
            "underlyingSymbol": "Underlying Symbol",
            "delistingDate": "Delisting Date",
            "prevTicker": "Previous Ticker",
            "tickerChangeDate": "Ticker Change Date",
            "prevExchange": "Previous Exchange",
            "exchangeTransferDate": "Exchange Transfer Date",
        }
    )

    return FinalDataFrame
