# StockMarketAnalysis

# Description
This project will take stock market data (and from other sources, including from other capital markets) and perform analysis using machine learning.

So far, the stocks file downloads stock market data including prices, company financials, and holders and downloads it into relational a PostgreSQL db.
Since it is using the yfinance python module, a caching system is being developed to limit the number of requests made. This system analyses a request
and determines whether that information can already be found in the database, or if the information was recently downloaded.

