# -*- coding: utf-8 -*-
"""
Created on Thu May 18 22:58:12 2017

@author: c0redumb
"""

# To make print working for Python2/3
from __future__ import print_function
import requests
import time
from datetime import datetime, date, timezone, timedelta
from enum import Enum
from os.path import expanduser
from os import environ
import logging

logging.basicConfig(level=getattr(logging, environ.get('LOGLEVEL', 'INFO').strip().upper()))

'''
Starting on May 2017, Yahoo financial has terminated its service on
the well used EOD data download without warning. This is confirmed
by Yahoo employee in forum posts.

Yahoo financial EOD data, however, still works on Yahoo financial pages.
These download links uses a "crumb" for authentication with a cookie "B".
This code is provided to obtain such matching cookie and crumb.
'''

default_useragent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'

class EventType(Enum):
    QUOTE = HISTORY = 'history'
    DIVIDEND = DIV = 'div'
    SPLIT = 'split'

class YahooQuote(object):
    def __init__(self, crumb=None, useragent=default_useragent, session=None):
        self.session = requests.Session() if session is None else session
        self.session.headers['User-Agent'] = useragent
        self.session.headers['Accept'] =  'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        self.crumb = crumb or self._get_crumb()

    def _get_crumb(self):
        '''
        This function perform a query and extract the matching crumb.
        '''

        r = self.session.get('https://query2.finance.yahoo.com/v1/test/getcrumb')
        r.raise_for_status()
        return r.text

    def csv(self, tickers, events=EventType.QUOTE, begindate=None, enddate=None, headers=True, max_rows=1, autoextend_days=7, sep=','):
        if isinstance(tickers, str):
            tickers = tickers,
        if isinstance(events, EventType):
            events = events._value_

        now = int(time.time())
        if enddate is None:
            enddate = now
        if begindate is None:
            begindate = now - 86400

        for ticker in tickers:
            while True:
                r = self.session.get('https://query2.finance.yahoo.com/v8/finance/chart/' + ticker,
                                     params = dict(period1=begindate, period2=enddate, events=events, interval='1d', crumb=self.crumb))
                if r.ok:
                    chart = r.json()['chart']
                    err = chart.get('error')
                    result = chart.get('result')[0]
                    if 'timestamp' not in result and autoextend_days > 0:
                        # go back one more day to try to find data
                        begindate -= 86400
                        autoextend_days -= 1
                        logging.debug('Moving start time back by 1 day to find real data (will try up to %d more days)' % autoextend_days)
                        continue
                    break

                try:
                    err = r.json()['chart']['error']
                    code = err['code']
                    desc = err['description']
                except Exception as exc:
                    raise RuntimeError(ticker, f'Could not parse error from response ({r.status_code} {r.reason}): {r.text}') from None
                else:
                    raise RuntimeError(ticker, err['description'], err['code'])
            else:
                # will only get here if we didn't get success, autoextend, or raise an error
                r.raise_for_status()

            #print(r.cookies, r.url)

            tz = timezone(timedelta(seconds=result['meta']['gmtoffset']), result['meta']['exchangeTimezoneName'])
            rows = list(zip(
                [ticker] * len(result['timestamp']),
                (datetime.fromtimestamp(ts, tz).date() for ts in result['timestamp']),
                result['indicators']['quote'][0]['open'],
                result['indicators']['quote'][0]['high'],
                result['indicators']['quote'][0]['low'],
                result['indicators']['quote'][0]['close'],
                result['indicators']['adjclose'][0]['adjclose'],
                result['indicators']['quote'][0]['volume'],
            ))

            # Remove all-'null' rows that YQ is now sometimes returning
            rows = [row for row in rows if any(f is not None for f in row[2:])]

            if headers:
                # only include the header row in output once
                yield sep.join(['Symbol', 'Open', 'High', 'Low', 'Close', 'Adjusted Close', 'Volume'])+'\n'
                headers = False
            yield from (sep.join(map(str, row))+'\n' for row in rows[-max_rows if max_rows is not None else None:])
