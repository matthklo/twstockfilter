#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function
from google.cloud import datastore
from WorkPool import WorkPool
from DividendCrawlingJob import DividendCrawlingJob
from CDYSCrawlingJob import CDYSCrawlingJob

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

    years = range(g_year, g_year - 6, -1)

    for e in entities.itervalues():
        dd = json.loads(e['dd'])
        rd = json.loads(e['rd'])

        # td/cd/sd
        try:
            d = dd[str(years[0] % 100)]
            cd = d[0]
            sd = d[1]
            td = cd + sd
            e.update({'td':td,'cd':cd,'sd':sd})
        except:
            if g_verbose:
                print('Info: Insufficient data for computing td/cd/sd values for stock_id: ' + e['id'])
        
        # pt/eps
        try:
            r = rd[str(years[0] % 100)]
            pt = r[0]
            eps = r[1]
            e.update({'pt':pt,'eps':eps})
        except:
            if g_verbose:
                print('Info: Insufficient data for computing pt/eps values for stock_id: ' + e['id'])
        
        # td3/cd3/sd3
        try:
            d = ( dd[str(years[0] % 100)], dd[str(years[1] % 100)], dd[str(years[2] % 100)] )
            cd3 = (d[0][0] + d[1][0] + d[2][0]) / 3.0
            sd3 = (d[0][1] + d[1][1] + d[2][1]) / 3.0
            td3 = cd3 + sd3
            e.update({'td3':td3,'cd3':cd3,'sd3':sd3})
        except:
            if g_verbose:
                print('Info: Insufficient data for computing td3/cd3/sd3 values for stock_id: ' + e['id'])

        # pt3/eps3
        try:
            r = ( rd[str(years[0] % 100)], rd[str(years[1] % 100)], rd[str(years[2] % 100)] )
            pt3 = (r[0][0] + r[1][0] + r[2][0]) / 3.0
            eps3 = (r[0][1] + r[1][1] + r[2][1]) / 3.0
            e.update({'pt3':pt3,'eps3':eps3})
        except:
            if g_verbose:
                print('Info: Insufficient data for computing pt3/eps3 values for stock_id: ' + e['id'])
        
        # td5/cd5/sd5
        try:
            d = ( dd[str(years[0] % 100)], dd[str(years[1] % 100)], dd[str(years[2] % 100)], dd[str(years[3] % 100)], dd[str(years[4] % 100)] )
            cd5 = (d[0][0] + d[1][0] + d[2][0] + d[3][0] + d[4][0]) / 5.0
            sd5 = (d[0][1] + d[1][1] + d[2][1] + d[3][1] + d[4][1]) / 5.0
            td5 = cd5 + sd5
            e.update({'td5':td5,'cd5':cd5,'sd5':sd5})
        except:
            if g_verbose:
                print('Info: Insufficient data for computing td5/cd5/sd5 values for stock_id: ' + e['id'])

        # pt5/eps5
        try:
            r = ( rd[str(years[0] % 100)], rd[str(years[1] % 100)], rd[str(years[2] % 100)], rd[str(years[3] % 100)], rd[str(years[4] % 100)] )
            pt5 = (r[0][0] + r[1][0] + r[2][0] + r[3][0] + r[4][0]) / 5.0
            eps5 = (r[0][1] + r[1][1] + r[2][1] + r[3][1] + r[4][1]) / 5.0
            e.update({'pt5':pt5,'eps5':eps5})
        except:
            if g_verbose:
                print('Info: Insufficient data for computing pt5/eps5 values for stock_id: ' + e['id'])
        
        b.put(e)
        bcnt += 1
        if bcnt >= 500:
            b.commit()
            b = g_gdclient.batch()
            b.begin()
            bcnt = 0
    
    if bcnt > 0:
        b.commit()

def commit_raw_data(entities, data):
    stock_dict = data[g_year]
    
    b = g_gdclient.batch()
    b.begin()
    bcnt = 0
    years = range(g_year, g_year - 6, -1)

    for s in stock_dict.iteritems():
        stock_id = s[0]

        # Build dd/rd content
        dd = {}
        rd = {}
        for y in years:
            if stock_id in data[y]:
                d = data[y][stock_id]
                dd[str(y % 100)] = [ d['cash_dividend'], d['stock_dividend'] ]
                rd[str(y % 100)] = [ d['profit'], d['eps'] ]

        if stock_id not in entities:
            # Need to create a new entity for this item
            e = datastore.Entity(key=g_gdclient.key('tw_stock_data',stock_id), exclude_from_indexes=('dd','rd'))
            e.update({
                'id': stock_id,
                'name': s[1]['name'],
                'dd': json.dumps(dd),
                'rd': json.dumps(rd)
            })
            entities[stock_id] = e
            b.put(e)
        else:
            e = entities[stock_id]
            e.update({'dd': json.dumps(dd), 'rd': json.dumps(rd)})
            b.put(e)

        bcnt += 1
        if bcnt >= 500:
            b.commit()
            b = g_gdclient.batch()
            b.begin()
            bcnt = 0
    
    if bcnt > 0:
        b.commit()
            

def fetch_data(years):
    tick_start = time.time()

    # Create a work pool, append crawling jobs and wait.
    wp = WorkPool()
    for y in years:
        j = DividendCrawlingJob(y)
        wp.append_job(j)
    wp.start()

    # Collect the results
    done_cnt = 0
    result = {}
    while done_cnt < len(years):
        j = wp.retrieve_job()
        if None == j:
            time.sleep(0.1)
            continue

        done_cnt += 1

        if j.web_req_success == False:
            print('Warning: DividendCrawlingJob failed on web request. Data year: ' + str(j.year))
        elif j.parse_success == False:
            print('Warning: DividendCrawlingJob failed on parsing. Data year: ' + str(j.year))
        else:
            if g_verbose:
                print('Info: DividendCrawlingJob: %d entries for data year %d' % (len(j.data), j.year))
            result[j.year] = j.data

    # Shutdown work pool
    wp.join()

    # Check if all data are fetched. Return all or none.
    if len(years) != len(result):
        if g_verbose:
            print('Warning: Mismatched data set count. Expected: %d, Fetched: %d' % (len(years), len(result)))
        return None

    if g_verbose:
        print('Info: fetch_data() costs %f seconds' % (time.time() - tick_start))

    return result

def fetch_cdys_data(entities):
    tick_start = time.time()

    # Create a work pool, append crawling jobs and wait.
    job_cnt = 0
    wp = WorkPool()
    for e in entities.itervalues():
        j = CDYSCrawlingJob(e['id'])
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
                print('Warning: CDYSCrawlingJob failed on web request. Stock id: ' + str(j.target_id))
        elif j.parse_success == False:
            if g_verbose:
                print('Warning: CDYSCrawlingJob failed on parsing. Stock id: ' + str(j.target_id))
        else:
            result[j.target_id] = j.data

    # Shutdown work pool
    wp.join()

    if g_verbose:
        print('Info: fetch_cdys_data() costs %f seconds' % (time.time() - tick_start))

    return result

def commit_cdys_data(entities, data):
    b = g_gdclient.batch()
    b.begin()
    bcnt = 0

    for e in entities.itervalues():
        if e['id'] not in data:
            continue
        
        d = data[e['id']]
        if isinstance(d,int):
            e.update({'cdys':d})
        b.put(e)

        bcnt += 1
        if bcnt >= 500:
            b.commit()
            b = g_gdclient.batch()
            b.begin()
            bcnt = 0
    
    if bcnt > 0:
        b.commit()

def show_usage():
    print('Usage: %s [-v] [-n] [-d] [-y year] [-q stock id]' % sys.argv[0])
    print('    [-v] Show verbose log.')
    print('    [-n] No data fetching.')
    print('    [-d] Delete all data from database then exit.')
    print('    [-y] Specify the lastest data year. (Default: last year)')
    print('    [-q] Query given stock id.')
    print('    [-c] Fetch & update CDYS data only.')

if __name__ == '__main__':
    # Check if GOOGLE_APPLICATION_CREDENTIALS environment variable has been set
    if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
        print('Error: Missing "GOOGLE_APPLICATION_CREDENTIALS" environment variable.', file=sys.stderr)
        sys.exit(1)

    # Parse command line arguments
    try:
        pairs, remaining = getopt.getopt(sys.argv[1:], 'nvdy:q:hc')
    except getopt.GetoptError as e:
        print('Error: ' + str(e), file=sys.stderr)
        show_usage()
        sys.exit(1)

    g_verbose = False
    g_data_fetching = True
    g_year = datetime.date.today().year - 1
    g_gdclient = datastore.Client()
    g_delete = False
    g_cdys_only = False

    for p in pairs:
        if p[0] == '-v':
            g_verbose = True
        elif p[0] == '-n':
            g_data_fetching = False
        elif p[0] == '-d':
            g_delete = True
        elif p[0] == '-y':
            g_year = int(p[1])
        elif p[0] == '-q':
            q = g_gdclient.query(kind='tw_stock_data')
            q.add_filter('id', '=', p[1])
            for e in q.fetch():
                print(str(e))
            sys.exit(0)
        elif p[0] == '-h':
            show_usage()
            sys.exit(0)
        elif p[0] == '-c':
            g_cdys_only = True

    # Perform data deletion when requested.
    if g_delete:
        b = g_gdclient.batch()
        b.begin()
        bcnt = 0

        for e in g_gdclient.query(kind='tw_stock_data').fetch():
            b.delete(e.key)
            bcnt += 1
            if bcnt >= 500:
                b.commit()
                if g_verbose:
                    print('Info: Deleting %d keys from database (kind = "tw_stock_data") ' % bcnt)
                b = g_gdclient.batch()
                b.begin()
                bcnt = 0
        
        if bcnt > 0:
            b.commit()
            if g_verbose:
                print('Info: Deleting %d keys from database (kind = "tw_stock_data") ' % bcnt)
        
        sys.exit(0)
        
    # Fetch existing entities from database. Could be empty if just deleted all data.
    entities = {}
    for e in g_gdclient.query(kind='tw_stock_data').fetch():
        entities[e['id']] = e

    # Fetch & update CDYS data
    cdys_data = fetch_cdys_data(entities)
    commit_cdys_data(entities, cdys_data)
    if g_cdys_only:
        sys.exit(0)

    if g_data_fetching:
        # Decide the year range of data.
        years = range(g_year, g_year - 6, -1)

        if g_verbose:
            print('Info: Fetching data for year range: ' + str(years))

        # Fetch & commit
        data = fetch_data(years)
        commit_raw_data(entities, data)

    # Compute & commit
    if len(entities) > 0:
        if g_verbose:
            print('Info: Computing %d entities ...' % len(entities))
        commit_computed_data(entities)

        
