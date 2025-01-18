### <u>**Description**</u>
---

This program retrieves every available equity ticker from Yahoo Finance. As of January 17, 2025, it can collect more than 50,000 tickers and all associated information. Since Yahoo Finance data is inherently delayed, speed was not the primary concern during development. On a typical connection, the script takes about three minutes to fetch all ticker data, generating a CSV file that is approximately 40 MB in size. A sample CSV (dated January 17, 2025) is included in the repository ZIP.

![Yahoo Finance](<insert_image_url>)

### <u>**Data Points Retrieved**</u>
---

```
[
 'Ask',
 'Ask Size',
 'Average Analyst Rating',
 'Average Daily Volume 10 Day',
 'Average Daily Volume 3 Month',
 'Bid',
 'Bid Size',
 'Book Value',
 'Corporate Actions',
 'Country',
 'Crypto Tradeable',
 'Currency',
 'Custom Price Alert Confidence',
 'Display Type',
 'Dividend Rate',
 'Dividend Yield',
 'EPS Current Year',
 'EPS Forward',
 'EPS Trailing Twelve Months',
 'ESG Populated',
 'Earnings Call Timestamp End',
 'Earnings Call Timestamp Start',
 'Earnings Timestamp',
 'Earnings Timestamp End',
 'Earnings Timestamp Start',
 'Exchange',
 'Exchange Data Delayed By',
 'Exchange Timezone Name',
 'Exchange Timezone Short Name',
 'Fifty Day Average',
 'Fifty Day Average Change',
 'Fifty Day Average Change Percent',
 'Fifty Two Week Change Percent',
 'Fifty Two Week High',
 'Fifty Two Week High Change',
 'Fifty Two Week High Change Percent',
 'Fifty Two Week Low',
 'Fifty Two Week Low Change',
 'Fifty Two Week Low Change Percent',
 'Fifty Two Week Range',
 'Financial Currency',
 'First Trade Date Milliseconds',
 'Forward PE',
 'Full Exchange Name',
 'GMT Offset Milliseconds',
 'Has Pre Post Market Data',
 'Industry',
 'Is Earnings Date Estimate',
 'Language',
 'Long Name',
 'Market',
 'Market Capitalization',
 'Market State',
 'Message Board ID',
 'Name Change Date',
 'Previous Name',
 'Price EPS Current Year',
 'Price Hint',
 'Price To Book',
 'Quote Source Name',
 'Quote Type',
 'Region',
 'Regular Market Change',
 'Regular Market Change Percent',
 'Regular Market Day High',
 'Regular Market Day Low',
 'Regular Market Day Range',
 'Regular Market Open',
 'Regular Market Previous Close',
 'Regular Market Price',
 'Regular Market Time',
 'Regular Market Volume',
 'Sector',
 'Shares Outstanding',
 'Short Name',
 'Source Interval',
 'Ticker',
 'Tradeable',
 'Trailing Annual Dividend Rate',
 'Trailing Annual Dividend Yield',
 'Trailing PE',
 'Triggerable',
 'Two Hundred Day Average',
 'Two Hundred Day Average Change',
 'Two Hundred Day Average Change Percent'
]
```

### <u>**Program Overview**</u>
---

The program intercepts specific HTTP requests from Yahoo Finance to gather these data points.

#### **Current Status**

- **Automation Attempt**: 
  - Successfully automated the retrieval of required cookies and parameters via SeleniumWire.
  
- **Issue Encountered**:  
  - The target request does not always appear in SeleniumWire’s intercepted requests, likely due to Yahoo’s bot detection and blocking.
  
- **Additional Attempts**:  
  - Using Undetected Chromedriver did not resolve the issue.  
  - Proxies were considered but not tested.  
  
- **Request for Input**:  
  - Seeking suggestions for further automation to make the program more accessible and user-friendly.
  
- **Current Solution**:  
  - Developed a manual tutorial that ensures the program remains functional.

