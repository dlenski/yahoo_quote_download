# yahoo_quote_download

Starting on May 2017, Yahoo financial has terminated its service on the well used EOD data download without warning. This is confirmed by Yahoo employee in forum posts.

Yahoo financial EOD data, however, still works on Yahoo financial pages. These download links uses a "crumb" for authentication. This code is provided to obtain such matching crumb. This code also downloads end of day stock quote from Yahoo finance.

Once the crumb is obtained, the querying URL is as following:

```
https://query1.finance.yahoo.com/v7/finance/download/TTTT?period1=pppppppp&period2=qqqqqqqq&interval=1d&events=eeeeeeee&crumb=cccccccc
```

where

- TTTT - Ticker (e.g., AAPL, MSFT, etc.)
- pppppppp - Period1 is the timestamp (POSIX time stamp) of the beginning date
- qqqqqqqq - Period2 is the timestamp (POSIX time stamp) of the ending date
- eeeeeeee - Event, can be one of 'history', 'div', or 'split'
- cccccccc - Crumb

The CSV file downloaded through the new API has a few data and format differences from the CSV file from the original iChart source. If you plan to use the downloaded data with code that used to process the data from old API, please check your code to make sure that these differences are taken care of.

1. The historical data of Open, High, and Low are already **adjusted**. In older API download, these data fields are not adjusted.
   * Update 06/22/2017: The adjustment of the historical data obtained through this new API now take into account of both dividends and splits. But it is done incorrectly, so that the adjusted close price sometimes falls outside of the adjust high / low.
   * Update 07/05/2017: Apparently, the close is adjusted for both dividends and splits, while the open/high/low are only adjusted for splits.
2. The order of data fields in each row is slightly different. The fields of the new API are as following (note that the order of the last two fields are swapped from before).
```
Date, Open, High, Low, Close, Adjusted Close, Volume
```
3. The order of the rows for historical quote by the new API is **chronical** (vs counter-chronical as the old API).
4. The order of the rows for splits/dividends seems random and is not chronically ordered.
5. There are a number of holes (dates with prices marked as NULL), even in some of the SP500 stocks.

# Usage

The first time you run `yqd`, it will extract a crumb from the web
application and save it in `~/.yahooquotes`.

See `yqd --help` for complete options list. Example of output in TSV format, only showing the
latest day's results for each ticker symbol:

```
$ yqd --tsv --latest FB AAPL AMZN NFLX GOOG  2> /dev/null
Symbol	Date	Open	High	Low	Close	Adj Close	Volume
FB	2022-01-20	323.899994	327.820007	315.980011	316.559998	316.559998	16925030
AAPL	2022-01-20	166.979996	169.639999	164.229996	164.509995	164.509995	91420515
AMZN	2022-01-20	3135.320068	3160.000000	3027.020020	3033.350098	3033.350098	3403343
NFLX	2022-01-20	517.750000	526.609985	506.929993	508.250000	508.250000	12658993
GOOG	2022-01-20	2730.280029	2758.239990	2663.280029	2670.129883	2670.129883	1089036
```

It can easily copy output to the clipboard in a form that LibreOffice Calc and Excel will accept using:

```
$ yqd --tsv --latest FB AAPL AMZN NFLX GOOG | xclip -sel clip
Got cached crumb from ~/.yahooquotes
```

# License

Note: This code is available through "Simplified BSD License".
