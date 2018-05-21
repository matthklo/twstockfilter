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
    print('Usage: %s [-p property] [-t threshold] [-s property] [-e encoding] [-f] [-r]' % sys.argv[0])
    print('  or')
    print('Usage: %s [-q stock id]' % sys.argv[0])
    print('    [-p] Property used for filtering. Default: cdy5')
    print('    [-t] Threshold value for filtering property. Default: 5.0')
    print('    [-s] Property used for sorting in descending order. Default: property used for -p')
    print('    [-e] Encoding for output. Default: utf-8')
    print('    [-f] Show stocks only listed in ETF. Need portfolio/portfolio.json')
    print('    [-r] Show tracking status. Need portfolio/tracking.json')
    print('    [-q] Query for a single stock by id')

def show_info_for_stock(e):
    def safe_get(e, p, v = 0.0):
        r = e.get(p, v)
        if r == None:
            return v
        return r

    print('Stock: %s (%s)' % (e['id'].encode(enc), e['name'].encode(enc)))
    print('    price: %.2f, roe: %.2f%%\t' % (safe_get(e, 'price'), safe_get(e, 'roe')))
    print('    pt/3/5:\t%.2f, %.2f, %.2f' % (safe_get(e, 'pt'), safe_get(e, 'pt3'), safe_get(e, 'pt5')))
    print('    eps/3/5:\t%.2f, %.2f, %.2f' % (safe_get(e, 'eps'), safe_get(e, 'eps3'), safe_get(e, 'eps5')))
    print('    dy/3/5:\t%.2f%%, %.2f%%, %.2f%%' % (safe_get(e, 'dy'), safe_get(e, 'dy3'), safe_get(e, 'dy5')))
    print('    cdy/3/5:\t%.2f%%, %.2f%%, %.2f%%' % (safe_get(e, 'cdy'), safe_get(e, 'cdy3'), safe_get(e, 'cdy5')))
    print('    ady/3/5:\t%.2f%%, %.2f%%, %.2f%%' % (safe_get(e, 'ady'), safe_get(e, 'ady3'), safe_get(e, 'ady5')))
    print('    cd/3/5:\t%.2f, %.2f, %.2f' % (safe_get(e, 'cd'), safe_get(e, 'cd3'), safe_get(e, 'cd5')))
    print('    sd/3/5:\t%.2f, %.2f, %.2f' % (safe_get(e, 'sd'), safe_get(e, 'sd3'), safe_get(e, 'sd5')))

if __name__ == '__main__':
    # Check if GOOGLE_APPLICATION_CREDENTIALS environment variable has been set
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        print('Error: Missing "GOOGLE_APPLICATION_CREDENTIALS" environment variable.', file=sys.stderr)
        sys.exit(1)

    try:
        pairs, remaining = getopt.getopt(sys.argv[1:], 'p:t:e:hrfq:s:')
    except getopt.GetoptError as e:
        print('Error: ' + str(e), file=sys.stderr)
        show_usage()
        sys.exit(1)

    fprop = 'cdy5'
    sprop = None
    fval = 5.0
    enc = 'utf-8'
    tracking = False
    etfonly = False
    query = None

    for p in pairs:
        if p[0] == '-p':
            fprop = p[1]
        elif p[0] == '-t':
            fval = float(p[1])
        elif p[0] == '-e':
            enc = p[1]
        elif p[0] == '-r':
            tracking = True
        elif p[0] == '-f':
            etfonly = True
        elif p[0] == '-q':
            query = p[1]
        elif p[0] == '-s':
            sprop = p[1]
        elif p[0] == '-h':
            show_usage()
            sys.exit(0)

    if None == sprop:
        sprop = fprop

    if etfonly:
        sdict = load_portfolio('portfolio/portfolio.json')
    if tracking:
        td = load_tracking_file('portfolio/tracking.json')

    client = datastore.Client()

    if None != query:
        q = client.query(kind='tw_stock_data')
        q.add_filter('id', '=', query)
        found = False
        for e in q.fetch():
            found = True
            show_info_for_stock(e)

        if not found:
            print('No match.')
            sys.exit(1)
        sys.exit(0)

    q = client.query(kind='tw_stock_data')
    q.add_filter(fprop, '>=', fval)
    elist = []
    for e in q.fetch():
        try:
            sid = int(e["id"])
        except:
            continue

        # Skip stocks which has already stopped trading.
        if 'price' not in e:
            continue

        if etfonly:
            if sid in sdict:
                elist.append(e)
        else:
            elist.append(e)

    slist = sorted(elist, key=lambda item: item.get(sprop, 0.0), reverse=True)
    
    print('Matched Count: ' + str(len(slist)) + ', Sort:' + sprop)
    for e in slist:
        pt5 = 0.0
        if 'pt5' in e:
            pt5 = e['pt5']

        try:
            eid = e['id'].encode(enc)
            ename = e['name'].encode(enc)

            if etfonly:
                print('%s %s\t[dy/5: %.2f%%, %.2f%%] [cdy/5: %.2f%%, %.2f%%] pr: %.2f, pt5: %.2f, roe: %.2f%%, ref: %d' % 
                    (eid, ename, e['dy'], e['dy5'], e['cdy'], e['cdy5'],
                    e['price'], pt5, e['roe'], sdict[int(e['id'])]['refcnt']))
            else:
                print('%s %s\t[dy/5: %.2f%%, %.2f%%] [cdy/5: %.2f%%, %.2f%%] pr: %.2f, pt5: %.2f, roe: %.2f%%' % 
                    (eid, ename, e['dy'], e['dy5'], e['cdy'], e['cdy5'],
                    e['price'], pt5, e['roe']))
        except UnicodeEncodeError as exp:
            print('UnicodeEncodeError when outputing data for stock ' + e['id'].encode(enc))
        except Exception as exp:
            print('Exception raised when outputing data for stock %s(%s): %s Message: %s' % 
                (eid, ename, str(type(exp)), str(exp)))
    
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
                print('%s %s\tTracking DY: %.2f%%, Tracking CDY: %.2f%%, PriceDiff: %.2f%%' % (e['id'].encode(enc), e['name'].encode(enc), tdy, tcdy, tdiff))
                break
