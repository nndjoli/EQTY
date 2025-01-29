"""
This module retrieves financial Data for a given Ticker symbol from Yahoo Finance,
processes it into a structured format, and stores it in a MongoDB DataBase.
It supports refreshing Data if outdated and provides easy access to various
financial metrics. The financial Data is stored in a Collection named after the
Ticker symbol in a DataBase named "Financials". The financial metrics are stored
as Key-Value pairs in a dictionary, with the Ticker symbol and the last update
timestamp included.
"""

import datetime
import pymongo
import requests
import re

# Local module import for Yahoo credentials
from . import Credentials as YFCredentials

# Retrieval of credentials
Credentials = YFCredentials.Get()
Cookies = Credentials.Cookies
Crumb = Credentials.Crumb
Headers = Credentials.Headers

FundamentalsOptionsMapping = [
    "annualAmortization",
    "annualAmortizationOfIntangiblesIncomeStatement",
    "annualAverageDilutionEarnings",
    "annualBasicAccountingChange",
    "annualBasicAverageShares",
    "annualBasicContinuousOperations",
    "annualBasicDiscontinuousOperations",
    "annualBasicEPS",
    "annualBasicEPSOtherGainsLosses",
    "annualBasicExtraordinary",
    "annualContinuingAndDiscontinuedBasicEPS",
    "annualContinuingAndDiscontinuedDilutedEPS",
    "annualCostOfRevenue",
    "annualDepletionIncomeStatement",
    "annualDepreciationAmortizationDepletionIncomeStatement",
    "annualDepreciationAndAmortizationInIncomeStatement",
    "annualDepreciationIncomeStatement",
    "annualDilutedAccountingChange",
    "annualDilutedAverageShares",
    "annualDilutedContinuousOperations",
    "annualDilutedDiscontinuousOperations",
    "annualDilutedEPS",
    "annualDilutedEPSOtherGainsLosses",
    "annualDilutedExtraordinary",
    "annualDilutedNIAvailtoComStockholders",
    "annualDividendPerShare",
    "annualEBIT",
    "annualEBITDA",
    "annualEarningsFromEquityInterest",
    "annualEarningsFromEquityInterestNetOfTax",
    "annualExciseTaxes",
    "annualGainOnSaleOfBusiness",
    "annualGainOnSaleOfPPE",
    "annualGainOnSaleOfSecurity",
    "annualGeneralAndAdministrativeExpense",
    "annualGrossProfit",
    "annualImpairmentOfCapitalAssets",
    "annualInsuranceAndClaims",
    "annualInterestExpense",
    "annualInterestExpenseNonOperating",
    "annualInterestIncome",
    "annualInterestIncomeNonOperating",
    "annualMinorityInterests",
    "annualNetIncome",
    "annualNetIncomeCommonStockholders",
    "annualNetIncomeContinuousOperations",
    "annualNetIncomeDiscontinuousOperations",
    "annualNetIncomeExtraordinary",
    "annualNetIncomeFromContinuingAndDiscontinuedOperation",
    "annualNetIncomeFromContinuingOperationNetMinorityInterest",
    "annualNetIncomeFromTaxLossCarryforward",
    "annualNetIncomeIncludingNoncontrollingInterests",
    "annualNetInterestIncome",
    "annualNetNonOperatingInterestIncomeExpense",
    "annualNormalizedBasicEPS",
    "annualNormalizedDilutedEPS",
    "annualNormalizedEBITDA",
    "annualNormalizedIncome",
    "annualOperatingExpense",
    "annualOperatingIncome",
    "annualOperatingRevenue",
    "annualOtherGandA",
    "annualOtherIncomeExpense",
    "annualOtherNonOperatingIncomeExpenses",
    "annualOtherOperatingExpenses",
    "annualOtherSpecialCharges",
    "annualOtherTaxes",
    "annualOtherunderPreferredStockDividend",
    "annualPreferredStockDividends",
    "annualPretaxIncome",
    "annualProvisionForDoubtfulAccounts",
    "annualReconciledCostOfRevenue",
    "annualReconciledDepreciation",
    "annualRentAndLandingFees",
    "annualRentExpenseSupplemental",
    "annualReportedNormalizedBasicEPS",
    "annualReportedNormalizedDilutedEPS",
    "annualResearchAndDevelopment",
    "annualRestructuringAndMergernAcquisition",
    "annualSalariesAndWages",
    "annualSecuritiesAmortization",
    "annualSellingAndMarketingExpense",
    "annualSellingGeneralAndAdministration",
    "annualSpecialIncomeCharges",
    "annualTaxEffectOfUnusualItems",
    "annualTaxLossCarryforwardBasicEPS",
    "annualTaxLossCarryforwardDilutedEPS",
    "annualTaxProvision",
    "annualTaxRateForCalcs",
    "annualTotalExpenses",
    "annualTotalOperatingIncomeAsReported",
    "annualTotalOtherFinanceCost",
    "annualTotalRevenue",
    "annualTotalUnusualItems",
    "annualTotalUnusualItemsExcludingGoodwill",
    "annualWriteOff",
    "annualAccountsPayable",
    "annualAccountsReceivable",
    "annualAccruedInterestReceivable",
    "annualAccumulatedDepreciation",
    "annualAdditionalPaidInCapital",
    "annualAllowanceForDoubtfulAccountsReceivable",
    "annualAssetsHeldForSaleCurrent",
    "annualAvailableForSaleSecurities",
    "annualBuildingsAndImprovements",
    "annualCapitalLeaseObligations",
    "annualCapitalStock",
    "annualCashAndCashEquivalents",
    "annualCashCashEquivalentsAndShortTermInvestments",
    "annualCashEquivalents",
    "annualCashFinancial",
    "annualCommercialPaper",
    "annualCommonStock",
    "annualCommonStockEquity",
    "annualConstructionInProgress",
    "annualCurrentAccruedExpenses",
    "annualCurrentAssets",
    "annualCurrentCapitalLeaseObligation",
    "annualCurrentDebt",
    "annualCurrentDebtAndCapitalLeaseObligation",
    "annualCurrentDeferredAssets",
    "annualCurrentDeferredLiabilities",
    "annualCurrentDeferredRevenue",
    "annualCurrentDeferredTaxesAssets",
    "annualCurrentDeferredTaxesLiabilities",
    "annualCurrentLiabilities",
    "annualCurrentNotesPayable",
    "annualCurrentProvisions",
    "annualDefinedPensionBenefit",
    "annualDerivativeProductLiabilities",
    "annualDividendsPayable",
    "annualDuefromRelatedPartiesCurrent",
    "annualDuefromRelatedPartiesNonCurrent",
    "annualDuetoRelatedPartiesCurrent",
    "annualDuetoRelatedPartiesNonCurrent",
    "annualEmployeeBenefits",
    "annualFinancialAssets",
    "annualFinancialAssetsDesignatedasFairValueThroughProfitorLossTotal",
    "annualFinishedGoods",
    "annualFixedAssetsRevaluationReserve",
    "annualForeignCurrencyTranslationAdjustments",
    "annualGainsLossesNotAffectingRetainedEarnings",
    "annualGeneralPartnershipCapital",
]


def RenameKey(Key):
    """
    Insert spaces before uppercase letters and capitalize each word.
    Example: 'annualTaxRateForCalcs' -> 'Annual Tax Rate For Calcs'.
    """
    SpacedKey = re.sub(r"([A-Z])", r" \1", Key)
    return SpacedKey.title()


def GetFinancials(Ticker):
    """
    Retrieve raw financial Data from the Yahoo Finance Fundamentals Timeseries API for a given Ticker.

    :param Ticker: The Ticker symbol for which to retrieve Data.
    :return: A dictionary of raw financial Data keyed by the metric name.
    """
    NowTimestamp = int(datetime.datetime.now().timestamp())
    BaseURL = "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/"
    TypeString = ",".join(FundamentalsOptionsMapping)

    url = (
        f"{BaseURL}{Ticker}?&formatted=false&lang=en-US&region=US&"
        f"corsDomain=finance.yahoo.com&crumb={Crumb}&period1=0&period2={NowTimestamp}"
        f"&type={TypeString}&merge=false&padTimeSeries=false"
    )

    try:
        Response = requests.get(url, headers=Headers, cookies=Cookies)
        Response.raise_for_status()  # Raises an HTTPError if the Response was unsuccessful
        Data = Response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error during the HTTP request: {e}")
        return {}
    except ValueError:
        print("Error converting the Response to JSON.")
        return {}

    # Parse the JSON Response to extract relevant financial Data
    Results = Data.get("timeseries", {}).get("result", [])
    if not Results:
        print("No Results found in the Response.")
        return {}

    Financials = {}
    for Item in Results:
        MetaTypes = Item.get("meta", {}).get("type", [])
        if MetaTypes:
            KeyName = MetaTypes[0]
            DataKeys = set(Item.keys()) - {"meta", "timestamp"}
            if DataKeys:
                # Extract the actual financial Data field
                DataKey = DataKeys.pop()
                Financials[KeyName] = Item[DataKey]
            else:
                Financials[KeyName] = None

    # Transform the raw financial structure for clarity
    StructuredFinancials = {}
    for Key, Value in Financials.items():
        NewKey = RenameKey(Key)
        if isinstance(Value, list):
            # Build a list of Data points for each financial metric
            StructuredFinancials[NewKey] = []
            for DataPoint in Value:
                Entry = {
                    "Data ID": DataPoint.get("dataId"),
                    "As Of Date": DataPoint.get("asOfDate"),
                    "Period Type": DataPoint.get("periodType"),
                    "Currency": DataPoint.get("currencyCode"),
                    "Value": DataPoint.get("reportedValue", {}).get("raw"),
                    "Formatted Value": DataPoint.get("reportedValue", {}).get(
                        "fmt"
                    ),
                }
                StructuredFinancials[NewKey].append(Entry)
        else:
            StructuredFinancials[NewKey] = Value

    # Attach Ticker and timestamp info
    StructuredFinancials["Ticker"] = Ticker
    StructuredFinancials["Last Update"] = datetime.datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return StructuredFinancials


def StoreFinancials(AllFinancials):
    """
    Store financial Data in MongoDB.

    :param AllFinancials: A dictionary or list of dictionaries containing
                           the financial Data to be stored.
    """
    if not isinstance(AllFinancials, list):
        AllFinancials = [AllFinancials]

    try:
        Ticker = AllFinancials[0].get("Ticker") if AllFinancials else None
        if not Ticker:
            print("Unable to store: no Ticker found.")
            return

        # Connect to MongoDB and store documents
        Client = pymongo.MongoClient()
        DataBase = Client["Financials"]
        Collection = DataBase[Ticker]
        Collection.insert_many(AllFinancials)
        print(f"All {Ticker} Financials have been stored in the DataBase.")
        Client.close()
    except Exception as e:
        print(f"Error storing Financials: {e}")


def Financials(Ticker=None, Key=None):
    """
    Retrieve and manage financial Data from the local MongoDB or Yahoo Finance.

    1. If 'Ticker' is not provided, print a message and return an empty dict.
    2. Connect to MongoDB and try to find existing Data for this Ticker.
    3. If Data is found, check the 'Last Update' timestamp:
       - If older than 7 days, refresh by downloading new Data from Yahoo Finance.
       - Otherwise, use existing Data.
    4. If Data is not found at all, download and store it before returning.
    5. If 'Key' is provided, return only that specific metric. Otherwise, return all Data.

    :param Ticker: The Ticker symbol to retrieve Data for.
    :param Key: (Optional) A specific metric to retrieve.
    :return: A dictionary of Financials (or a specific Value if 'Key' is specified).
    """
    if Ticker is None:
        print("Please specify a Ticker.")
        return {}

    try:
        Client = pymongo.MongoClient()
        DataBase = Client["Financials"]
        Collection = DataBase[Ticker]

        Document = Collection.find_one({"Ticker": Ticker})
        Client.close()

        # If Data already exists, check its freshness
        if Document:
            LastUpdateString = Document.get("Last Update")
            NeedUpdate = True

            if LastUpdateString:
                try:
                    LastUpdateDatetime = datetime.datetime.strptime(
                        LastUpdateString, "%Y-%m-%d %H:%M:%S"
                    )
                    Delta = datetime.datetime.now() - LastUpdateDatetime
                    # Refresh if older than 7 days
                    if Delta.days < 7:
                        NeedUpdate = False
                except ValueError:
                    # If date format is unknown, force a refresh
                    pass

            # If we need an update, download new Data
            if NeedUpdate:
                print(
                    "The Data is older than one week or invalid date format. Downloading new Data..."
                )
                UpddatedFinancials = GetFinancials(Ticker)
                if UpddatedFinancials:
                    StoreFinancials(UpddatedFinancials)
                    Document = UpddatedFinancials
                else:
                    print(f"Unable to update Data for Ticker: {Ticker}.")

            # Return the requested Value (or entire Document)
            if Key is None:
                return dict(sorted(Document.items()))
            else:
                Value = Document.get(Key)
                if Value is None:
                    print(f"'{Key}' not found for Ticker: '{Ticker}'.")
                return Value

        else:
            # No Document found, download from Yahoo Finance and store
            NewFinancials = GetFinancials(Ticker)
            if not NewFinancials:
                print(f"Unable to retrieve Data for the Ticker: {Ticker}.")
                return {}
            StoreFinancials(NewFinancials)

            if Key is None:
                return dict(sorted(NewFinancials.items()))
            else:
                Value = NewFinancials.get(Key)
                if Value is None:
                    print(f"'{Key}' not found for Ticker: '{Ticker}'.")
                return Value

    except Exception as e:
        print(f"Error retrieving {Ticker} Financials: {e}")
        return {}


def AnnualAmortization(Ticker):
    """
    Retrieve 'Annual Amortization' Data for the given Ticker.
    """
    return Financials(Ticker).get("Annual Amortization", None)


def AnnualAmortizationOfIntangiblesIncomeStatement(Ticker):
    """
    Retrieve 'Annual Amortization Of Intangibles Income Statement' Data.
    """
    return Financials(Ticker).get(
        "Annual Amortization Of Intangibles Income Statement", None
    )


def AnnualAverageDilutionEarnings(Ticker):
    """
    Retrieve 'Annual Average Dilution Earnings' Data.
    """
    return Financials(Ticker).get("Annual Average Dilution Earnings", None)


def AnnualBasicAccountingChange(Ticker):
    """
    Retrieve 'Annual Basic Accounting Change' Data.
    """
    return Financials(Ticker).get("Annual Basic Accounting Change", None)


def AnnualBasicAverageShares(Ticker):
    """
    Retrieve 'Annual Basic Average Shares' Data.
    """
    return Financials(Ticker).get("Annual Basic Average Shares", None)


def AnnualBasicContinuousOperations(Ticker):
    """
    Retrieve 'Annual Basic Continuous Operations' Data.
    """
    return Financials(Ticker).get("Annual Basic Continuous Operations", None)


def AnnualBasicDiscontinuousOperations(Ticker):
    """
    Retrieve 'Annual Basic Discontinuous Operations' Data.
    """
    return Financials(Ticker).get("Annual Basic Discontinuous Operations", None)


def AnnualBasicEPS(Ticker):
    """
    Retrieve 'Annual Basic E P S' Data.
    """
    return Financials(Ticker).get("Annual Basic E P S", None)


def AnnualBasicEPSOtherGainsLosses(Ticker):
    """
    Retrieve 'Annual Basic E P S Other Gains Losses' Data.
    """
    return Financials(Ticker).get("Annual Basic E P S Other Gains Losses", None)


def AnnualBasicExtraordinary(Ticker):
    """
    Retrieve 'Annual Basic Extraordinary' Data.
    """
    return Financials(Ticker).get("Annual Basic Extraordinary", None)


def AnnualContinuingAndDiscontinuedBasicEPS(Ticker):
    """
    Retrieve 'Annual Continuing And Discontinued Basic E P S' Data.
    """
    return Financials(Ticker).get(
        "Annual Continuing And Discontinued Basic E P S", None
    )


def AnnualContinuingAndDiscontinuedDilutedEPS(Ticker):
    """
    Retrieve 'Annual Continuing And Discontinued Diluted E P S' Data.
    """
    return Financials(Ticker).get(
        "Annual Continuing And Discontinued Diluted E P S", None
    )


def AnnualCostOfRevenue(Ticker):
    """
    Retrieve 'Annual Cost Of Revenue' Data.
    """
    return Financials(Ticker).get("Annual Cost Of Revenue", None)


def AnnualDepletionIncomeStatement(Ticker):
    """
    Retrieve 'Annual Depletion Income Statement' Data.
    """
    return Financials(Ticker).get("Annual Depletion Income Statement", None)


def AnnualDepreciationAmortizationDepletionIncomeStatement(Ticker):
    """
    Retrieve 'Annual Depreciation Amortization Depletion Income Statement' Data.
    """
    return Financials(Ticker).get(
        "Annual Depreciation Amortization Depletion Income Statement", None
    )


def AnnualDepreciationAndAmortizationInIncomeStatement(Ticker):
    """
    Retrieve 'Annual Depreciation And Amortization In Income Statement' Data.
    """
    return Financials(Ticker).get(
        "Annual Depreciation And Amortization In Income Statement", None
    )


def AnnualDepreciationIncomeStatement(Ticker):
    """
    Retrieve 'Annual Depreciation Income Statement' Data.
    """
    return Financials(Ticker).get("Annual Depreciation Income Statement", None)


def AnnualDilutedAccountingChange(Ticker):
    """
    Retrieve 'Annual Diluted Accounting Change' Data.
    """
    return Financials(Ticker).get("Annual Diluted Accounting Change", None)


def AnnualDilutedAverageShares(Ticker):
    """
    Retrieve 'Annual Diluted Average Shares' Data.
    """
    return Financials(Ticker).get("Annual Diluted Average Shares", None)


def AnnualDilutedContinuousOperations(Ticker):
    """
    Retrieve 'Annual Diluted Continuous Operations' Data.
    """
    return Financials(Ticker).get("Annual Diluted Continuous Operations", None)


def AnnualDilutedDiscontinuousOperations(Ticker):
    """
    Retrieve 'Annual Diluted Discontinuous Operations' Data.
    """
    return Financials(Ticker).get(
        "Annual Diluted Discontinuous Operations", None
    )


def AnnualDilutedEPS(Ticker):
    """
    Retrieve 'Annual Diluted E P S' Data.
    """
    return Financials(Ticker).get("Annual Diluted E P S", None)


def AnnualDilutedEPSOtherGainsLosses(Ticker):
    """
    Retrieve 'Annual Diluted E P S Other Gains Losses' Data.
    """
    return Financials(Ticker).get(
        "Annual Diluted E P S Other Gains Losses", None
    )


def AnnualDilutedExtraordinary(Ticker):
    """
    Retrieve 'Annual Diluted Extraordinary' Data.
    """
    return Financials(Ticker).get("Annual Diluted Extraordinary", None)


def AnnualDilutedNIAvailtoComStockholders(Ticker):
    """
    Retrieve 'Annual Diluted N I Availto Com Stockholders' Data.
    """
    return Financials(Ticker).get(
        "Annual Diluted N I Availto Com Stockholders", None
    )


def AnnualDividendPerShare(Ticker):
    """
    Retrieve 'Annual Dividend Per Share' Data.
    """
    return Financials(Ticker).get("Annual Dividend Per Share", None)


def AnnualEBIT(Ticker):
    """
    Retrieve 'Annual E B I T' Data.
    """
    return Financials(Ticker).get("Annual E B I T", None)


def AnnualEBITDA(Ticker):
    """
    Retrieve 'Annual E B I T D A' Data.
    """
    return Financials(Ticker).get("Annual E B I T D A", None)


def AnnualEarningsFromEquityInterest(Ticker):
    """
    Retrieve 'Annual Earnings From Equity Interest' Data.
    """
    return Financials(Ticker).get("Annual Earnings From Equity Interest", None)


def AnnualEarningsFromEquityInterestNetOfTax(Ticker):
    """
    Retrieve 'Annual Earnings From Equity Interest Net Of Tax' Data.
    """
    return Financials(Ticker).get(
        "Annual Earnings From Equity Interest Net Of Tax", None
    )


def AnnualExciseTaxes(Ticker):
    """
    Retrieve 'Annual Excise Taxes' Data.
    """
    return Financials(Ticker).get("Annual Excise Taxes", None)


def AnnualGainOnSaleOfBusiness(Ticker):
    """
    Retrieve 'Annual Gain On Sale Of Business' Data.
    """
    return Financials(Ticker).get("Annual Gain On Sale Of Business", None)


def AnnualGainOnSaleOfPPE(Ticker):
    """
    Retrieve 'Annual Gain On Sale Of P P E' Data.
    """
    return Financials(Ticker).get("Annual Gain On Sale Of P P E", None)


def AnnualGainOnSaleOfSecurity(Ticker):
    """
    Retrieve 'Annual Gain On Sale Of Security' Data.
    """
    return Financials(Ticker).get("Annual Gain On Sale Of Security", None)


def AnnualGeneralAndAdministrativeExpense(Ticker):
    """
    Retrieve 'Annual General And Administrative Expense' Data.
    """
    return Financials(Ticker).get(
        "Annual General And Administrative Expense", None
    )


def AnnualGrossProfit(Ticker):
    """
    Retrieve 'Annual Gross Profit' Data.
    """
    return Financials(Ticker).get("Annual Gross Profit", None)


def AnnualImpairmentOfCapitalAssets(Ticker):
    """
    Retrieve 'Annual Impairment Of Capital Assets' Data.
    """
    return Financials(Ticker).get("Annual Impairment Of Capital Assets", None)


def AnnualInsuranceAndClaims(Ticker):
    """
    Retrieve 'Annual Insurance And Claims' Data.
    """
    return Financials(Ticker).get("Annual Insurance And Claims", None)


def AnnualInterestExpense(Ticker):
    """
    Retrieve 'Annual Interest Expense' Data.
    """
    return Financials(Ticker).get("Annual Interest Expense", None)


def AnnualInterestExpenseNonOperating(Ticker):
    """
    Retrieve 'Annual Interest Expense Non Operating' Data.
    """
    return Financials(Ticker).get("Annual Interest Expense Non Operating", None)


def AnnualInterestIncome(Ticker):
    """
    Retrieve 'Annual Interest Income' Data.
    """
    return Financials(Ticker).get("Annual Interest Income", None)


def AnnualInterestIncomeNonOperating(Ticker):
    """
    Retrieve 'Annual Interest Income Non Operating' Data.
    """
    return Financials(Ticker).get("Annual Interest Income Non Operating", None)


def AnnualMinorityInterests(Ticker):
    """
    Retrieve 'Annual Minority Interests' Data.
    """
    return Financials(Ticker).get("Annual Minority Interests", None)


def AnnualNetIncome(Ticker):
    """
    Retrieve 'Annual Net Income' Data.
    """
    return Financials(Ticker).get("Annual Net Income", None)


def AnnualNetIncomeCommonStockholders(Ticker):
    """
    Retrieve 'Annual Net Income Common Stockholders' Data.
    """
    return Financials(Ticker).get("Annual Net Income Common Stockholders", None)


def AnnualNetIncomeContinuousOperations(Ticker):
    """
    Retrieve 'Annual Net Income Continuous Operations' Data.
    """
    return Financials(Ticker).get(
        "Annual Net Income Continuous Operations", None
    )


def AnnualNetIncomeDiscontinuousOperations(Ticker):
    """
    Retrieve 'Annual Net Income Discontinuous Operations' Data.
    """
    return Financials(Ticker).get(
        "Annual Net Income Discontinuous Operations", None
    )


def AnnualNetIncomeExtraordinary(Ticker):
    """
    Retrieve 'Annual Net Income Extraordinary' Data.
    """
    return Financials(Ticker).get("Annual Net Income Extraordinary", None)


def AnnualNetIncomeFromContinuingAndDiscontinuedOperation(Ticker):
    """
    Retrieve 'Annual Net Income From Continuing And Discontinued Operation' Data.
    """
    return Financials(Ticker).get(
        "Annual Net Income From Continuing And Discontinued Operation", None
    )


def AnnualNetIncomeFromContinuingOperationNetMinorityInterest(Ticker):
    """
    Retrieve 'Annual Net Income From Continuing Operation Net Minority Interest' Data.
    """
    return Financials(Ticker).get(
        "Annual Net Income From Continuing Operation Net Minority Interest",
        None,
    )


def AnnualNetIncomeFromTaxLossCarryforward(Ticker):
    """
    Retrieve 'Annual Net Income From Tax Loss Carryforward' Data.
    """
    return Financials(Ticker).get(
        "Annual Net Income From Tax Loss Carryforward", None
    )


def AnnualNetIncomeIncludingNoncontrollingInterests(Ticker):
    """
    Retrieve 'Annual Net Income Including Noncontrolling Interests' Data.
    """
    return Financials(Ticker).get(
        "Annual Net Income Including Noncontrolling Interests", None
    )


def AnnualNetInterestIncome(Ticker):
    """
    Retrieve 'Annual Net Interest Income' Data.
    """
    return Financials(Ticker).get("Annual Net Interest Income", None)


def AnnualNetNonOperatingInterestIncomeExpense(Ticker):
    """
    Retrieve 'Annual Net Non Operating Interest Income Expense' Data.
    """
    return Financials(Ticker).get(
        "Annual Net Non Operating Interest Income Expense", None
    )


def AnnualNormalizedBasicEPS(Ticker):
    """
    Retrieve 'Annual Normalized Basic E P S' Data.
    """
    return Financials(Ticker).get("Annual Normalized Basic E P S", None)


def AnnualNormalizedDilutedEPS(Ticker):
    """
    Retrieve 'Annual Normalized Diluted E P S' Data.
    """
    return Financials(Ticker).get("Annual Normalized Diluted E P S", None)


def AnnualNormalizedEBITDA(Ticker):
    """
    Retrieve 'Annual Normalized E B I T D A' Data.
    """
    return Financials(Ticker).get("Annual Normalized E B I T D A", None)


def AnnualNormalizedIncome(Ticker):
    """
    Retrieve 'Annual Normalized Income' Data.
    """
    return Financials(Ticker).get("Annual Normalized Income", None)


def AnnualOperatingExpense(Ticker):
    """
    Retrieve 'Annual Operating Expense' Data.
    """
    return Financials(Ticker).get("Annual Operating Expense", None)


def AnnualOperatingIncome(Ticker):
    """
    Retrieve 'Annual Operating Income' Data.
    """
    return Financials(Ticker).get("Annual Operating Income", None)


def AnnualOperatingRevenue(Ticker):
    """
    Retrieve 'Annual Operating Revenue' Data.
    """
    return Financials(Ticker).get("Annual Operating Revenue", None)


def AnnualOtherGandA(Ticker):
    """
    Retrieve 'Annual Other Gand A' Data.
    """
    return Financials(Ticker).get("Annual Other Gand A", None)


def AnnualOtherIncomeExpense(Ticker):
    """
    Retrieve 'Annual Other Income Expense' Data.
    """
    return Financials(Ticker).get("Annual Other Income Expense", None)


def AnnualOtherNonOperatingIncomeExpenses(Ticker):
    """
    Retrieve 'Annual Other Non Operating Income Expenses' Data.
    """
    return Financials(Ticker).get(
        "Annual Other Non Operating Income Expenses", None
    )


def AnnualOtherOperatingExpenses(Ticker):
    """
    Retrieve 'Annual Other Operating Expenses' Data.
    """
    return Financials(Ticker).get("Annual Other Operating Expenses", None)


def AnnualOtherSpecialCharges(Ticker):
    """
    Retrieve 'Annual Other Special Charges' Data.
    """
    return Financials(Ticker).get("Annual Other Special Charges", None)


def AnnualOtherTaxes(Ticker):
    """
    Retrieve 'Annual Other Taxes' Data.
    """
    return Financials(Ticker).get("Annual Other Taxes", None)


def AnnualOtherunderPreferredStockDividend(Ticker):
    """
    Retrieve 'Annual Otherunder Preferred Stock Dividend' Data.
    """
    return Financials(Ticker).get(
        "Annual Otherunder Preferred Stock Dividend", None
    )


def AnnualPreferredStockDividends(Ticker):
    """
    Retrieve 'Annual Preferred Stock Dividends' Data.
    """
    return Financials(Ticker).get("Annual Preferred Stock Dividends", None)


def AnnualPretaxIncome(Ticker):
    """
    Retrieve 'Annual Pretax Income' Data.
    """
    return Financials(Ticker).get("Annual Pretax Income", None)


def AnnualProvisionForDoubtfulAccounts(Ticker):
    """
    Retrieve 'Annual Provision For Doubtful Accounts' Data.
    """
    return Financials(Ticker).get(
        "Annual Provision For Doubtful Accounts", None
    )


def AnnualReconciledCostOfRevenue(Ticker):
    """
    Retrieve 'Annual Reconciled Cost Of Revenue' Data.
    """
    return Financials(Ticker).get("Annual Reconciled Cost Of Revenue", None)


def AnnualReconciledDepreciation(Ticker):
    """
    Retrieve 'Annual Reconciled Depreciation' Data.
    """
    return Financials(Ticker).get("Annual Reconciled Depreciation", None)


def AnnualRentAndLandingFees(Ticker):
    """
    Retrieve 'Annual Rent And Landing Fees' Data.
    """
    return Financials(Ticker).get("Annual Rent And Landing Fees", None)


def AnnualRentExpenseSupplemental(Ticker):
    """
    Retrieve 'Annual Rent Expense Supplemental' Data.
    """
    return Financials(Ticker).get("Annual Rent Expense Supplemental", None)


def AnnualReportedNormalizedBasicEPS(Ticker):
    """
    Retrieve 'Annual Reported Normalized Basic E P S' Data.
    """
    return Financials(Ticker).get(
        "Annual Reported Normalized Basic E P S", None
    )


def AnnualReportedNormalizedDilutedEPS(Ticker):
    """
    Retrieve 'Annual Reported Normalized Diluted E P S' Data.
    """
    return Financials(Ticker).get(
        "Annual Reported Normalized Diluted E P S", None
    )


def AnnualResearchAndDevelopment(Ticker):
    """
    Retrieve 'Annual Research And Development' Data.
    """
    return Financials(Ticker).get("Annual Research And Development", None)


def AnnualRestructuringAndMergernAcquisition(Ticker):
    """
    Retrieve 'Annual Restructuring And Mergern Acquisition' Data.
    """
    return Financials(Ticker).get(
        "Annual Restructuring And Mergern Acquisition", None
    )


def AnnualSalariesAndWages(Ticker):
    """
    Retrieve 'Annual Salaries And Wages' Data.
    """
    return Financials(Ticker).get("Annual Salaries And Wages", None)


def AnnualSecuritiesAmortization(Ticker):
    """
    Retrieve 'Annual Securities Amortization' Data.
    """
    return Financials(Ticker).get("Annual Securities Amortization", None)


def AnnualSellingAndMarketingExpense(Ticker):
    """
    Retrieve 'Annual Selling And Marketing Expense' Data.
    """
    return Financials(Ticker).get("Annual Selling And Marketing Expense", None)


def AnnualSellingGeneralAndAdministration(Ticker):
    """
    Retrieve 'Annual Selling General And Administration' Data.
    """
    return Financials(Ticker).get(
        "Annual Selling General And Administration", None
    )


def AnnualSpecialIncomeCharges(Ticker):
    """
    Retrieve 'Annual Special Income Charges' Data.
    """
    return Financials(Ticker).get("Annual Special Income Charges", None)


def AnnualTaxEffectOfUnusualItems(Ticker):
    """
    Retrieve 'Annual Tax Effect Of Unusual Items' Data.
    """
    return Financials(Ticker).get("Annual Tax Effect Of Unusual Items", None)


def AnnualTaxLossCarryforwardBasicEPS(Ticker):
    """
    Retrieve 'Annual Tax Loss Carryforward Basic E P S' Data.
    """
    return Financials(Ticker).get(
        "Annual Tax Loss Carryforward Basic E P S", None
    )


def AnnualTaxLossCarryforwardDilutedEPS(Ticker):
    """
    Retrieve 'Annual Tax Loss Carryforward Diluted E P S' Data.
    """
    return Financials(Ticker).get(
        "Annual Tax Loss Carryforward Diluted E P S", None
    )


def AnnualTaxProvision(Ticker):
    """
    Retrieve 'Annual Tax Provision' Data.
    """
    return Financials(Ticker).get("Annual Tax Provision", None)


def AnnualTaxRateForCalcs(Ticker):
    """
    Retrieve 'Annual Tax Rate For Calcs' Data.
    """
    return Financials(Ticker).get("Annual Tax Rate For Calcs", None)


def AnnualTotalExpenses(Ticker):
    """
    Retrieve 'Annual Total Expenses' Data.
    """
    return Financials(Ticker).get("Annual Total Expenses", None)


def AnnualTotalOperatingIncomeAsReported(Ticker):
    """
    Retrieve 'Annual Total Operating Income As Reported' Data.
    """
    return Financials(Ticker).get(
        "Annual Total Operating Income As Reported", None
    )


def AnnualTotalOtherFinanceCost(Ticker):
    """
    Retrieve 'Annual Total Other Finance Cost' Data.
    """
    return Financials(Ticker).get("Annual Total Other Finance Cost", None)


def AnnualTotalRevenue(Ticker):
    """
    Retrieve 'Annual Total Revenue' Data.
    """
    return Financials(Ticker).get("Annual Total Revenue", None)


def AnnualTotalUnusualItems(Ticker):
    """
    Retrieve 'Annual Total Unusual Items' Data.
    """
    return Financials(Ticker).get("Annual Total Unusual Items", None)


def AnnualTotalUnusualItemsExcludingGoodwill(Ticker):
    """
    Retrieve 'Annual Total Unusual Items Excluding Goodwill' Data.
    """
    return Financials(Ticker).get(
        "Annual Total Unusual Items Excluding Goodwill", None
    )


def AnnualWriteOff(Ticker):
    """
    Retrieve 'Annual Write Off' Data.
    """
    return Financials(Ticker).get("Annual Write Off", None)


def AnnualAccountsPayable(Ticker):
    """
    Retrieve 'Annual Accounts Payable' Data.
    """
    return Financials(Ticker).get("Annual Accounts Payable", None)


def AnnualAccountsReceivable(Ticker):
    """
    Retrieve 'Annual Accounts Receivable' Data.
    """
    return Financials(Ticker).get("Annual Accounts Receivable", None)


def AnnualAccruedInterestReceivable(Ticker):
    """
    Retrieve 'Annual Accrued Interest Receivable' Data.
    """
    return Financials(Ticker).get("Annual Accrued Interest Receivable", None)


def AnnualAccumulatedDepreciation(Ticker):
    """
    Retrieve 'Annual Accumulated Depreciation' Data.
    """
    return Financials(Ticker).get("Annual Accumulated Depreciation", None)


def AnnualAdditionalPaidInCapital(Ticker):
    """
    Retrieve 'Annual Additional Paid In Capital' Data.
    """
    return Financials(Ticker).get("Annual Additional Paid In Capital", None)


def AnnualAllowanceForDoubtfulAccountsReceivable(Ticker):
    """
    Retrieve 'Annual Allowance For Doubtful Accounts Receivable' Data.
    """
    return Financials(Ticker).get(
        "Annual Allowance For Doubtful Accounts Receivable", None
    )


def AnnualAssetsHeldForSaleCurrent(Ticker):
    """
    Retrieve 'Annual Assets Held For Sale Current' Data.
    """
    return Financials(Ticker).get("Annual Assets Held For Sale Current", None)


def AnnualAvailableForSaleSecurities(Ticker):
    """
    Retrieve 'Annual Available For Sale Securities' Data.
    """
    return Financials(Ticker).get("Annual Available For Sale Securities", None)


def AnnualBuildingsAndImprovements(Ticker):
    """
    Retrieve 'Annual Buildings And Improvements' Data.
    """
    return Financials(Ticker).get("Annual Buildings And Improvements", None)


def AnnualCapitalLeaseObligations(Ticker):
    """
    Retrieve 'Annual Capital Lease Obligations' Data.
    """
    return Financials(Ticker).get("Annual Capital Lease Obligations", None)


def AnnualCapitalStock(Ticker):
    """
    Retrieve 'Annual Capital Stock' Data.
    """
    return Financials(Ticker).get("Annual Capital Stock", None)


def AnnualCashAndCashEquivalents(Ticker):
    """
    Retrieve 'Annual Cash And Cash Equivalents' Data.
    """
    return Financials(Ticker).get("Annual Cash And Cash Equivalents", None)


def AnnualCashCashEquivalentsAndShortTermInvestments(Ticker):
    """
    Retrieve 'Annual Cash Cash Equivalents And Short Term Investments' Data.
    """
    return Financials(Ticker).get(
        "Annual Cash Cash Equivalents And Short Term Investments", None
    )


def AnnualCashEquivalents(Ticker):
    """
    Retrieve 'Annual Cash Equivalents' Data.
    """
    return Financials(Ticker).get("Annual Cash Equivalents", None)


def AnnualCashFinancial(Ticker):
    """
    Retrieve 'Annual Cash Financial' Data.
    """
    return Financials(Ticker).get("Annual Cash Financial", None)


def AnnualCommercialPaper(Ticker):
    """
    Retrieve 'Annual Commercial Paper' Data.
    """
    return Financials(Ticker).get("Annual Commercial Paper", None)


def AnnualCommonStock(Ticker):
    """
    Retrieve 'Annual Common Stock' Data.
    """
    return Financials(Ticker).get("Annual Common Stock", None)


def AnnualCommonStockEquity(Ticker):
    """
    Retrieve 'Annual Common Stock Equity' Data.
    """
    return Financials(Ticker).get("Annual Common Stock Equity", None)


def AnnualConstructionInProgress(Ticker):
    """
    Retrieve 'Annual Construction In Progress' Data.
    """
    return Financials(Ticker).get("Annual Construction In Progress", None)


def AnnualCurrentAccruedExpenses(Ticker):
    """
    Retrieve 'Annual Current Accrued Expenses' Data.
    """
    return Financials(Ticker).get("Annual Current Accrued Expenses", None)


def AnnualCurrentAssets(Ticker):
    """
    Retrieve 'Annual Current Assets' Data.
    """
    return Financials(Ticker).get("Annual Current Assets", None)


def AnnualCurrentCapitalLeaseObligation(Ticker):
    """
    Retrieve 'Annual Current Capital Lease Obligation' Data.
    """
    return Financials(Ticker).get(
        "Annual Current Capital Lease Obligation", None
    )


def AnnualCurrentDebt(Ticker):
    """
    Retrieve 'Annual Current Debt' Data.
    """
    return Financials(Ticker).get("Annual Current Debt", None)


def AnnualCurrentDebtAndCapitalLeaseObligation(Ticker):
    """
    Retrieve 'Annual Current Debt And Capital Lease Obligation' Data.
    """
    return Financials(Ticker).get(
        "Annual Current Debt And Capital Lease Obligation", None
    )


def AnnualCurrentDeferredAssets(Ticker):
    """
    Retrieve 'Annual Current Deferred Assets' Data.
    """
    return Financials(Ticker).get("Annual Current Deferred Assets", None)


def AnnualCurrentDeferredLiabilities(Ticker):
    """
    Retrieve 'Annual Current Deferred Liabilities' Data.
    """
    return Financials(Ticker).get("Annual Current Deferred Liabilities", None)


def AnnualCurrentDeferredRevenue(Ticker):
    """
    Retrieve 'Annual Current Deferred Revenue' Data.
    """
    return Financials(Ticker).get("Annual Current Deferred Revenue", None)


def AnnualCurrentDeferredTaxesAssets(Ticker):
    """
    Retrieve 'Annual Current Deferred Taxes Assets' Data.
    """
    return Financials(Ticker).get("Annual Current Deferred Taxes Assets", None)


def AnnualCurrentDeferredTaxesLiabilities(Ticker):
    """
    Retrieve 'Annual Current Deferred Taxes Liabilities' Data.
    """
    return Financials(Ticker).get(
        "Annual Current Deferred Taxes Liabilities", None
    )


def AnnualCurrentLiabilities(Ticker):
    """
    Retrieve 'Annual Current Liabilities' Data.
    """
    return Financials(Ticker).get("Annual Current Liabilities", None)


def AnnualCurrentNotesPayable(Ticker):
    """
    Retrieve 'Annual Current Notes Payable' Data.
    """
    return Financials(Ticker).get("Annual Current Notes Payable", None)


def AnnualCurrentProvisions(Ticker):
    """
    Retrieve 'Annual Current Provisions' Data.
    """
    return Financials(Ticker).get("Annual Current Provisions", None)


def AnnualDefinedPensionBenefit(Ticker):
    """
    Retrieve 'Annual Defined Pension Benefit' Data.
    """
    return Financials(Ticker).get("Annual Defined Pension Benefit", None)


def AnnualDerivativeProductLiabilities(Ticker):
    """
    Retrieve 'Annual Derivative Product Liabilities' Data.
    """
    return Financials(Ticker).get("Annual Derivative Product Liabilities", None)


def AnnualDividendsPayable(Ticker):
    """
    Retrieve 'Annual Dividends Payable' Data.
    """
    return Financials(Ticker).get("Annual Dividends Payable", None)


def AnnualDuefromRelatedPartiesCurrent(Ticker):
    """
    Retrieve 'Annual Duefrom Related Parties Current' Data.
    """
    return Financials(Ticker).get(
        "Annual Duefrom Related Parties Current", None
    )


def AnnualDuefromRelatedPartiesNonCurrent(Ticker):
    """
    Retrieve 'Annual Duefrom Related Parties Non Current' Data.
    """
    return Financials(Ticker).get(
        "Annual Duefrom Related Parties Non Current", None
    )


def AnnualDuetoRelatedPartiesCurrent(Ticker):
    """
    Retrieve 'Annual Dueto Related Parties Current' Data.
    """
    return Financials(Ticker).get("Annual Dueto Related Parties Current", None)


def AnnualDuetoRelatedPartiesNonCurrent(Ticker):
    """
    Retrieve 'Annual Dueto Related Parties Non Current' Data.
    """
    return Financials(Ticker).get(
        "Annual Dueto Related Parties Non Current", None
    )


def AnnualEmployeeBenefits(Ticker):
    """
    Retrieve 'Annual Employee Benefits' Data.
    """
    return Financials(Ticker).get("Annual Employee Benefits", None)


def AnnualFinancialAssets(Ticker):
    """
    Retrieve 'Annual Financial Assets' Data.
    """
    return Financials(Ticker).get("Annual Financial Assets", None)


def AnnualFinancialAssetsDesignatedasFairValueThroughProfitorLossTotal(Ticker):
    """
    Retrieve 'Annual Financial Assets Designatedas Fair Value Through Profitor Loss Total' Data.
    """
    return Financials(Ticker).get(
        "Annual Financial Assets Designatedas Fair Value Through Profitor Loss Total",
        None,
    )


def AnnualFinishedGoods(Ticker):
    """
    Retrieve 'Annual Finished Goods' Data.
    """
    return Financials(Ticker).get("Annual Finished Goods", None)


def AnnualFixedAssetsRevaluationReserve(Ticker):
    """
    Retrieve 'Annual Fixed Assets Revaluation Reserve' Data.
    """
    return Financials(Ticker).get(
        "Annual Fixed Assets Revaluation Reserve", None
    )


def AnnualForeignCurrencyTranslationAdjustments(Ticker):
    """
    Retrieve 'Annual Foreign Currency Translation Adjustments' Data.
    """
    return Financials(Ticker).get(
        "Annual Foreign Currency Translation Adjustments", None
    )


def AnnualGainsLossesNotAffectingRetainedEarnings(Ticker):
    """
    Retrieve 'Annual Gains Losses Not Affecting Retained Earnings' Data.
    """
    return Financials(Ticker).get(
        "Annual Gains Losses Not Affecting Retained Earnings", None
    )


def AnnualGeneralPartnershipCapital(Ticker):
    """
    Retrieve 'Annual General Partnership Capital' Data.
    """
    return Financials(Ticker).get("Annual General Partnership Capital", None)
