"""
This module retrieves authentication headers, cookies, and a crumb token
required to send authenticated requests to Yahoo Finance endpoints.
"""

import requests


class Get:

    def __init__(self):
        """
        Initializes the Credentials class by setting up headers, cookies, and crumb.
        Attributes:
            Headers (dict): The headers required for making requests.
            Cookies (dict): The cookies required for making requests.
            Crumb (str): The crumb value required for making requests.
        """

        self.Headers = self.GetHeaders()
        self.Cookies = self.GetCookies()
        self.Crumb = self.GetCrumb()

    def GetHeaders(self):
        """
        Generates HTTP headers for making requests.
        Returns:
            dict: A dictionary containing the User-Agent header.
        """

        UserAgentKey = "User-Agent"
        UserAgentValue = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"

        return {UserAgentKey: UserAgentValue}

    def GetCookies(self):
        """
        Retrieves Yahoo authentication cookies.
        This method sends a GET request to the Yahoo URL with the provided headers
        and retrieves the authentication cookies from the response. If no cookies
        are found in the response, an exception is raised.
        Returns:
            dict: A dictionary containing the cookie name and value.
        Raises:
            Exception: If the Yahoo authentication cookie is not obtained.
        """

        Headers = self.Headers
        URL = "https://fc.yahoo.com"

        Response = requests.get(URL, headers=Headers, allow_redirects=True)

        if not Response.cookies:
            raise Exception("Failed to obtain Yahoo auth cookie.")

        Cookies = list(Response.cookies)[0]
        Name, Value = Cookies.name, Cookies.value

        Cookies = {Name: Value}

        return Cookies

    def GetCrumb(self):
        """
        Retrieves the Yahoo crumb required for making authenticated requests to Yahoo Finance.
        This method sends a GET request to the Yahoo Finance API to obtain a crumb, which is
        necessary for certain API requests. The request includes headers and cookies for
        authentication.
        Returns:
            str: The Yahoo crumb as a string.
        Raises:
            Exception: If the request fails or the crumb cannot be retrieved.
        """

        Headers = self.Headers
        Cookies = self.Cookies

        URL = "https://query1.finance.yahoo.com/v1/test/getcrumb"

        Response = requests.get(
            URL, headers=Headers, cookies=Cookies, allow_redirects=True
        )

        if Response.status_code != 200:
            raise Exception("Failed to retrieve Yahoo crumb.")

        Crumb = Response.text

        if Crumb is None:
            raise Exception("Failed to retrieve Yahoo crumb.")

        return Crumb
