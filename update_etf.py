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

def show_usage():
    print('Usage: %s [-d]' % sys.argv[0])
    print('    [-d] Dry run.')

if __name__ == '__main__':
    # Check if GOOGLE_APPLICATION_CREDENTIALS environment variable has been set
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        print('Error: Missing "GOOGLE_APPLICATION_CREDENTIALS" environment variable.', file=sys.stderr)
        sys.exit(1)

    # Parse command line arguments
    try:
        pairs, remaining = getopt.getopt(sys.argv[1:], 'd')
    except getopt.GetoptError as e:
        print('Error: ' + str(e), file=sys.stderr)
        show_usage()
        sys.exit(1)

    dry_run = False

    for p in pairs:
        if p[0] == '-d':
            dry_run = True

    sdict = load_portfolio('portfolio/portfolio.json')

    client = datastore.Client()
    batch = client.batch()
    batch.begin()

    delcnt = 0
    addcnt = 0
    idset = set()

    q = client.query(kind='tw_stock_etf')
    for e in q.fetch():
        try:
            idnum = int(e['id'])
        except:
            continue
        
        idset.add(idnum)

        if idnum not in sdict:
            batch.delete(e.key)
            delcnt += 1
    
    for idnum in sdict:
        if idnum not in idset:
            e = datastore.Entity(key=client.key('tw_stock_etf', str(idnum)))
            e.update({'id': str(idnum)})
            batch.put(e)
            addcnt += 1

    print('Add: %d, Delete: %d' % (addcnt, delcnt))
    if dry_run:
        print('Skip commit during dry-run.')
    else:
        batch.commit()
    
