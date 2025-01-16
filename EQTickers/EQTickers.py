import pandas
import yfinance
import matplotlib.pyplot


class EQTYahoo:

    def __init__(self, csv_path):
        self.__Data__ = pandas.read_csv(csv_path)
        self.__Industries__ = [
            str(i) for i in self.__Data__["Industry"].unique().tolist()
        ]
        self.__Sectors__ = [str(i) for i in self.__Data__["Sector"].unique().tolist()]
        self.__Countries__ = [
            str(i) for i in self.__Data__["Country"].unique().tolist()
        ]
        self.__Currencies__ = [
            str(i) for i in self.__Data__["currency"].unique().tolist()
        ]
        self.__Exchanges__ = [
            str(i) for i in self.__Data__["Exchange"].unique().tolist()
        ]
        self.__ExchangesNames__ = [
            str(i) for i in self.__Data__["Exchange Name"].unique().tolist()
        ]
        self.__Regions__ = [str(i) for i in self.__Data__["Region"].unique().tolist()]

    def AvailableTickers(self):
        return self.__Data__

    def AvailableIndustries(self):
        return self.__Industries__

    def AvailableSectors(self):
        return sorted(self.__Sectors__)

    def AvailableCountries(self):
        return sorted(self.__Countries__)

    def AvailableCurrencies(self):
        return sorted(self.__Currencies__)

    def AvailableExchanges(self):
        return sorted(self.__Exchanges__)

    def AvailableExchangesNames(self):
        return sorted(self.__ExchangesNames__)

    def AvailableRegions(self):
        return sorted(self.__Regions__)

    def Get(
        self,
        region=None,
        country=None,
        exchange=None,
        exchange_name=None,
        currency=None,
        sector=None,
        industry=None,
        sorted="cap",
        length="all",
    ):
        tickers = self.__Data__.copy()

        if region is not None:
            tickers = tickers[tickers["Region"] == region]
        if country is not None:
            tickers = tickers[tickers["Country"] == country]

        if exchange is not None:
            tickers = tickers[tickers["Exchange"] == exchange]
        if exchange_name is not None:
            tickers = tickers[tickers["Exchange Name"] == exchange_name]
        if currency is not None:
            tickers = tickers[tickers["currency"] == currency]

        if sector is not None:
            tickers = tickers[tickers["Sector"] == sector]
        if industry is not None:
            tickers = tickers[tickers["Industry"] == industry]

        if sorted == "cap":
            tickers = tickers.sort_values(by="Market Cap", ascending=False)
        elif sorted == "alpha":
            tickers = tickers.sort_values(by="Ticker", ascending=True)
        else:
            print(
                "Invalid sorting method (cap or alpha). The default sorting method (cap) will be used."
            )
            tickers = tickers.sort_values(by="Market Cap", ascending=False)

        if isinstance(length, int):
            tickers = tickers.head(length)
        elif length == "all":
            pass
        else:
            print("Invalid length argument. The default length (all) will be used.")

        return tickers

    def GetData(
        self,
        region=None,
        country=None,
        exchange=None,
        exchange_name=None,
        currency=None,
        sector=None,
        industry=None,
        sorted="cap",
        length="all",
        start=None,
        end=None,
        actions=False,
        threads=10,
        ignore_tz=None,
        group_by="column",
        auto_adjust=True,
        back_adjust=False,
        repair=False,
        keepna=False,
        progress=True,
        period="max",
        interval="1d",
        prepost=False,
        proxy=None,
        rounding=False,
        timeout=10,
        session=None,
        multi_level_index=True,
    ):

        tickers = self.Get(
            region,
            country,
            exchange,
            exchange_name,
            currency,
            sector,
            industry,
            sorted,
            length,
        )

        if tickers.empty:
            print("No tickers found matching the given criteria.")
            return None

        tickers_list = tickers["Ticker"].tolist()
        if not tickers_list:
            print("No valid tickers in the DataFrame.")
            return None

        df = yfinance.download(
            tickers_list,
            start=start,
            end=end,
            actions=actions,
            threads=threads,
            ignore_tz=ignore_tz,
            group_by=group_by,
            auto_adjust=auto_adjust,
            back_adjust=back_adjust,
            repair=repair,
            keepna=keepna,
            progress=progress,
            period=period,
            interval=interval,
            prepost=prepost,
            proxy=proxy,
            rounding=rounding,
            timeout=timeout,
            session=session,
            multi_level_index=multi_level_index,
        )

        return df
