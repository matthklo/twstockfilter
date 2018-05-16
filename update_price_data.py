#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function
from google.cloud import datastore
from WorkPool import WorkPool
from PriceCrawlingJob import PriceCrawlingJob

import datetime
import getopt
import json
import os
import sys
import time

def commit_computed_data(entities):
    b = g_gdclient.batch()
    b.begin()
    bcnt = 0

    for e in entities.itervalues():
        if 'price' not in e:
            continue

        price = e['price']
        if price == 0.0:
            continue

        # cdy/3/5
        if 'cd' in e:
            e.update({'cdy': e['cd']*100.0/price})
        if 'cd3' in e:
            e.update({'cdy3': e['cd3']*100.0/price})
        if 'cd5' in e:
            e.update({'cdy5': e['cd5']*100.0/price})

        # dy/3/5
        if 'td' in e:
            e.update({'dy': e['td']*100.0/price})
        if 'td3' in e:
            e.update({'dy3': e['td3']*100.0/price})
        if 'td5' in e:
            e.update({'dy5': e['td5']*100.0/price})

        # ady/3/5
        ratio = price / 10.0
        if ('sd' in e) and ('cd' in e):
            e.update({'ady': (e['cd'] + e['sd']*ratio)*100.0/price})
        if ('sd3' in e) and ('cd3' in e):
            e.update({'ady3': (e['cd3'] + e['sd3']*ratio)*100.0/price})
        if ('sd5' in e) and ('cd5' in e):
            e.update({'ady5': (e['cd5'] + e['sd5']*ratio)*100.0/price})
        
        b.put(e)
        bcnt += 1
        if bcnt >= 500:
            b.commit()
            b = g_gdclient.batch()
            b.begin()
            bcnt = 0
    
    if bcnt > 0:
        b.commit()

def commit_price_data(entities, data):
    b = g_gdclient.batch()
    b.begin()
    bcnt = 0

    for e in entities.itervalues():
        if e['id'] not in data:
            continue
        
        d = data[e['id']]
        if 'roe' in d:
            e.update({'roe':d['roe']})
        if 'price' in d:
            e.update({'price':d['price']})
        b.put(e)

        bcnt += 1
        if bcnt >= 500:
            b.commit()
            b = g_gdclient.batch()
            b.begin()
            bcnt = 0
    
    if bcnt > 0:
        b.commit()
            

def fetch_data(entities):
    tick_start = time.time()

    # Create a work pool, append crawling jobs and wait.
    job_cnt = 0
    wp = WorkPool()
    for e in entities.itervalues():
        j = PriceCrawlingJob(e['id'])
        wp.append_job(j)
        job_cnt += 1
    wp.start()

    # Collect the results
    done_cnt = 0
    result = {}
    while done_cnt < job_cnt:
        j = wp.retrieve_job()
        if None == j:
            time.sleep(0.1)
            continue

        done_cnt += 1

        if j.web_req_success == False:
            if g_verbose:
                print('Warning: PriceCrawlingJob failed on web request. Stock id: ' + str(j.target_id))
        elif j.parse_success == False:
            if g_verbose:
                print('Warning: PriceCrawlingJob failed on parsing. Stock id: ' + str(j.target_id))
        else:
            result[j.target_id] = j.data

    # Shutdown work pool
    wp.join()

    if g_verbose:
        print('Info: fetch_data() costs %f seconds' % (time.time() - tick_start))

    return result


def show_usage():
    print('Usage: %s [-v] [-n] [-q stock id]' % sys.argv[0])
    print('    [-v] Show verbose log.')
    print('    [-n] No data fetching.')
    print('    [-q] Query given stock id. No database update.')

if __name__ == '__main__':
    # Check if GOOGLE_APPLICATION_CREDENTIALS environment variable has been set
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        print('Error: Missing "GOOGLE_APPLICATION_CREDENTIALS" environment variable.', file=sys.stderr)
        sys.exit(1)

    # Parse command line arguments
    try:
        pairs, remaining = getopt.getopt(sys.argv[1:], 'nvq:h')
    except getopt.GetoptError as e:
        print('Error: ' + str(e), file=sys.stderr)
        show_usage()
        sys.exit(1)

    g_verbose = False
    g_data_fetching = True
    g_gdclient = datastore.Client()

    for p in pairs:
        if p[0] == '-v':
            g_verbose = True
        elif p[0] == '-n':
            g_data_fetching = False
        elif p[0] == '-q':
            p = PriceCrawlingJob(p[1], g_verbose)
            p()
            if p.web_req_success and p.parse_success:
                print(str(p.data))
            else:
                print('Info: PriceCrawlingJob ends up with failure.')
            sys.exit(0)
        elif p[0] == '-h':
            show_usage()
            sys.exit(0)
        
    # Fetch existing entities from database. Could be empty if just deleted all data.
    entities = {}
    for e in g_gdclient.query(kind='tw_stock_data').fetch():
        entities[e['id']] = e

    if g_data_fetching:
        # Fetch & commit
        if g_verbose:
            print('Info: Fetching %d stocks.' % len(entities))
        data = fetch_data(entities)
        commit_price_data(entities, data)

    # Compute & commit
    if len(entities) > 0:
        if g_verbose:
            print('Info: Computing %d entities ...' % len(entities))
        commit_computed_data(entities)

        
