#!/usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import print_function
from google.cloud import datastore
import getopt
import json
import os
import sys

def load_portfolio(filename):
    sdict = {}
    try:
        with open(filename, 'r') as rf:
            pflist = json.load(rf, encoding='utf-8')
            for p in pflist:
                load_portfolio_file(p, sdict)
            return sdict
    except Exception as e:
        raise RuntimeError('Unable to load portfolio index file: ' + str(filename) + ', Exception:' + str(e))

def load_portfolio_file(filename, sdict):
    try:
        with open(filename, 'r') as rf:
            stock_list = json.load(rf, encoding='utf-8')
            for p in stock_list:
                if p['id'] not in sdict:
                    sdict[p['id']] = { 'name': p['name'], 'refcnt': 1 }
                else:
                    sdict[p['id']]['refcnt'] = sdict[p['id']]['refcnt'] + 1
      
    except Exception as e:
        raise RuntimeError('Unable to load portfolio file: ' + str(filename) + ', Exception:' + str(e))

def load_tracking_file(filename):
    try:
        with open(filename, 'r') as rf:
            return json.load(rf, encoding="utf-8")
    except:
        raise RuntimeError('Unable to load tracking file: ' + str(filename))

def show_usage():
    print('Usage: %s [-p property] [-t threshold] [-e encoding] [-r]' % sys.argv[0])
    print('    [-p] Property name used for filtering. Default: cdy5')
    print('    [-t] Threshold value for filtering property. Default: 5.0')
    print('    [-e] Encoding for output. Default: utf-8')
    print('    [-r] Show tracking status. Need portfolio/tracking.json')

if __name__ == '__main__':
    # Check if GOOGLE_APPLICATION_CREDENTIALS environment variable has been set
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        print('Error: Missing "GOOGLE_APPLICATION_CREDENTIALS" environment variable.', file=sys.stderr)
        sys.exit(1)

    try:
        pairs, remaining = getopt.getopt(sys.argv[1:], 'p:t:e:hr')
    except getopt.GetoptError as e:
        print('Error: ' + str(e), file=sys.stderr)
        show_usage()
        sys.exit(1)

    fprop = 'cdy5'
    fval = 5.0
    enc = 'utf-8'
    tracking = False

    for p in pairs:
        if p[0] == '-p':
            fprop = p[1]
        elif p[0] == '-t':
            fval = float(p[1])
        elif p[0] == '-e':
            enc = p[1]
        elif p[0] == '-r':
            tracking = True
        elif p[0] == '-h':
            show_usage()
            sys.exit(0)

    sdict = load_portfolio('portfolio/portfolio.json')
    if tracking:
        td = load_tracking_file('portfolio/tracking.json')

    client = datastore.Client()
    q = client.query(kind='tw_stock_data')
    q.add_filter(fprop, '>', fval)
    q.order = [ '-'+fprop ]
    elist = []
    for e in q.fetch():
        try:
            sid = int(e["id"])
        except:
            continue

        if sid in sdict:
            elist.append(e)
    
    print('Matched Count: ' + str(len(elist)))
    for e in elist:
        pt5 = 0.0
        if 'pt5' in e:
            pt5 = e['pt5']

        print('%s %s, [dy5: %.2f%%, dy: %.2f%%] [cdy5: %.2f%%, cdy: %.2f%%] [ady5: %.2f%%, ady: %.2f%%] pr: %.2f, pt5: %.2f, ref: %d' % 
            (e['id'].encode(enc), e['name'].encode(enc), e['dy5'], e['dy'], e['cdy5'], e['cdy'], e['ady5'], e['ady'],
            e['price'], pt5, sdict[int(e['id'])]['refcnt']))
    
    if tracking:
        print('\nTracking Items')
        #cdwp = td['config']['cd_warn']
        #pwp = td['config']['price_warn']
        for ti in td['items']:
            q = client.query(kind='tw_stock_data')
            q.add_filter('id', '=', str(ti['id']))
            for e in q.fetch():
                tdy = e['td'] / ti['price'] * 100.0
                tcdy = e['cd'] / ti['price'] * 100.0
                tdiff = (e['price'] - ti['price']) / ti['price'] * 100.0
                print('%s %s, Tracking DY: %.2f%%, Tracking CDY: %.2f%%, PriceDiff: %.2f%%' % (e['id'].encode(enc), e['name'].encode(enc), tdy, tcdy, tdiff))
                break
