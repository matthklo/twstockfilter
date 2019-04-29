#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function
import json
import re
import urllib2
import random
import time

"""
    CDYSCrawlingJob:
        可配合 WorkPool 使用的 Job 類別
        創建時帶入目標的 stock id，會向 GoodInfo! 網頁抓取該股已連續配發股利的年數 
        (CDYS: Consecutive Dividend Years)
"""

class CDYSCrawlingJob:

    """
        data source: "Goodinfo!台灣股市資訊網"
    """
    data_src_url = 'https://goodinfo.tw/StockInfo/StockDividendPolicy.asp?STOCK_ID='

    def __init__ ( self, stock_id ):
        # Build web request
        target_url = self.data_src_url + str(stock_id)
        self.web_req = urllib2.Request(target_url, None, headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36' })
        self.target_id = stock_id
        self.web_req_success = False
        self.parse_success = False
        self.data = 0
        self.myid = str(type(self)) + '_stock_' + str(stock_id)

    # Note: Called by dedicated thread of WorkPool
    def __call__ ( self ):

        #### Fire the web request.
        try:
            # Apply a random delay of 1 ~ 7 seconds to prevent getting banned by GoodInfo!
            time.sleep(random.random() * 6 + 1)
            result = urllib2.urlopen(self.web_req).read().decode('utf-8')
            self.web_req_success = True
        except urllib2.HTTPError as e:
            self.web_req_exception = e
        except Exception as e:
            print('CDYSCrawlingJob: Unexpected exception occurred when crawling for CDYS data. Exception: ' + str(e))

        #### Early abort on error
        if False == self.web_req_success:
            return

        #### Continue parsing CDYS data.

        try:
            m = re.search(u'連續([0-9]+)年配發股利', result)
            if (m != None) and (len(m.groups()) >= 1):
                self.data = int(m.group(1))
                self.parse_success = True

        except Exception as e:
            print('CDYSCrawlingJob: Unexpected exception occurred when parsing CDYS data. Exception: ' + str(e))

