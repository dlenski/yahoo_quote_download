import os, sys, time
import argparse
from .yqd import YahooQuote, EventType

def main():
    ets = [et._value_ for et in EventType]
    p = argparse.ArgumentParser()
    p.add_argument('-H', '--no-header', action='store_false', default=True, dest='header')
    p.add_argument('-e', '--events', choices=ets, default=ets[0])
    x = p.add_mutually_exclusive_group()
    x.add_argument('-d', '--days', type=int, default=1, help='Number of days of results to return (default %(default)s)')
    x.add_argument('-L', '--latest', action='store_true', help='Just return the one most-recent row for each ticker')
    p.add_argument('-t', '--tsv', action='store_const', const='\t', dest='sep', default=',',
                   help='Output TSV (tab-separated) rather than CSV (comma-separated)')
    p.add_argument('ticker', nargs='+')
    args = p.parse_args()

    try:
        crumb, = open(os.path.expanduser("~/.yahooquotes")).read().splitlines()
        print("Got cached crumb from ~/.yahooquotes", file=sys.stderr)
    except Exception:
        crumb = None
    yq = YahooQuote(crumb)
    if yq.crumb != crumb:
        with open(os.path.expanduser("~/.yahooquotes"), 'w') as f:
            f.write(yq.crumb)
        print("Cached crumb in ~/.yahooquotes", file=sys.stderr)

    if args.latest:
        begindate, max_rows = None, 1
    else:
        max_rows, begindate = None, int(time.time()-86400*args.days)

    try:
        sys.stdout.writelines( yq.csv(args.ticker, events=args.events, headers=args.header, begindate=begindate, max_rows=max_rows, sep=args.sep) )
    except RuntimeError as exc:
        ticker, desc, *code = exc.args
        p.error(f'Error fetching data for ticker {ticker}: {desc}')

if __name__=='__main__':
    main()
