#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function
import json
import re
import urllib2

"""
    DividendCrawlingJob:
        可配合 WorkPool 使用的 Job 類別
        創建時帶入提取的西元年份 Ex: 2017，當 __call__ 被呼叫時，會透過 web request 向
        data source 提取該年度所有台灣上市櫃股票的配股配息數據回來解析。
        解析後的資料儲存於 DividendCrawlingJob.data 中。格式為:

        {
          "2227": {
            "id": 2227,
            "name": "裕日車",
            "cash_dividend": 1.0,
            "stock_dividend": 1.0
          }
          .....
        }

    Note: 提取/解析數據的過程中有可能會發生異常，請先判斷 DividendCrawlingJob.web_req_success、
        以及 DividendCrawlingJob.parse_success 的值再決定是否採用 DividendCrawlingJob.data。
"""

class DividendCrawlingJob:

    """
        data source: "Goodinfo!台灣股市資訊網"
    """
    data_src_url = 'https://goodinfo.tw/StockInfo/StockDividendPolicyList.asp?MARKET_CAT=%E4%B8%8A%E5%B8%82&INDUSTRY_CAT=%E5%85%A8%E9%83%A8&YEAR='

    def __init__ ( self, year ):
        # Build web request
        target_url = self.data_src_url + str(year)
        self.web_req = urllib2.Request(target_url, None, headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36' })
        
        self.web_req_success = False
        self.parse_success = False
        self.data = {}
        self.myid = str(type(self)) + '_year_' + str(year)
        self.year = year

    # Note: Called by dedicated thread of WorkPool
    def __call__ ( self ):

        #### Fire the web request.
        try:
            result = urllib2.urlopen(self.web_req).read().decode('utf-8')
            self.web_req_success = True
        except urllib2.HTTPError as e:
            self.web_req_exception = e
        except Exception as e:
            print('DividendCrawlingJob: Unexpected exception occurred when crawling for devidend data. Exception: ' + str(e))

        #### Early abort on error
        if False == self.web_req_success:
            print('DividendCrawlingJob: Web request ended up as failed.')
            return

        #### Continue parsing dividend data.

        try:
            # Stripping Step 1. Strip out all HTML tags '<...>'
            t = re.sub(r'<[^>]+>', '', result)
            # Stripping Step 2. Strip all '\r', '\n', and '\t' characters.
            t = re.sub(r'[\r\t\n]', '', t)
            # Stripping Step 3. Split the remaining data with spaces (but keep empty tokens)
            raw_tokens = t.split(' ')

            # Extract entry for each stock. Use '上市' as an anchor.
            total_entries = []
            entry = []
            for token in raw_tokens:
                print('DividendCrawlingJob: token= ' + token)
                if token == u'上市':
                    total_entries.append(entry)
                    entry = []
                else:
                    entry.append(token)
            # Drop the first entry (gabage)
            total_entries = total_entries[1:] 

            """
                For now, every entry in total_entries is a full dividend info of a stock item.
                Index of some vital info:
                entry[1] : Stock ID           (string)
                entry[3] : Stock Name         (string)
                entry[5] : Report year        (int, should be the same as the 'self.year')
                entry[9] : Profit after taxed (float or None, unit: 億)
                entry[11] : EPS                (float or None)
                entry[17]: Cash dividend      (float)
                entry[23]: Stock dividend     (float)
            """

            for e in total_entries:
                # Sanity check
                if e[5].encode('utf-8') != str(self.year):
                    raise RuntimeError('Sanity check failed (report year not matched).')

                # Parse profit whenever possible
                try:
                    profit = float(e[9].replace(',',''))
                except:
                    profit = None

                # Parse eps whenever possible
                try:
                    eps = float(e[11])
                except:
                    eps = None

                self.data[e[1]] = { 
                    'id': e[1], 
                    'name': e[3], 
                    'year': int(e[5]), 
                    'profit': profit,
                    'eps' : eps,
                    'cash_dividend': float(e[17]), 
                    'stock_dividend': float(e[23]) 
                }

            self.parse_success = True

        except Exception as e:
            print('DividendCrawlingJob: Unexpected exception occurred when parsing devidend data. Exception: ' + str(e))

if __name__ == '__main__':
    # Entry point to quickly verify parsing result
    job = DividendCrawlingJob(2023)
    job()
    print(str(job.data['2816']))

