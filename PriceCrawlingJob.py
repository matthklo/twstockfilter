#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function
import re
import urllib2

class PriceCrawlingJob ():
    basic_info_url_format = 'https://tw.stock.yahoo.com/quote/%s/profile'
    price_url_format = 'https://tw.stock.yahoo.com/quote/%s' 

    def __init__ ( self, stock_id, debug=False ):
        self.target_id = stock_id
        self.debug = debug
        self.myid = str(type(self)) + '_stock_' + str(stock_id)
        self.web_req_success = False
        self.parse_success = False
        self.data = {}

    def __call__ ( self ):        
        try:
            self.basic_info_raw = self.do_fetch(self.basic_info_url_format % str(self.target_id))
            self.price_info_raw = self.do_fetch(self.price_url_format % str(self.target_id))
            self.web_req_success = True
        except Exception as e:
            if self.debug:
                print('Error: PriceCrawlingJob: Failed on perform web request for stock id: ' + str(self.target_id) + ', Exception: ' + str(e))
            return
        
        try:
            self.parse()
            self.parse_success = True
        except Exception as e:
            if self.debug:
                print('Error: PriceCrawlingJob: Failed on parsing for stock id: ' + str(self.target_id) + ', Exception: ' + str(e))
            return


    def do_fetch(self, url):
        if self.debug:
            print('Info: PriceCrawlingJob: Crawling web URL: ' + url)
        content = urllib2.urlopen(url)
        return unicode(content.read(), 'utf-8', 'ignore')

    def parse(self):
        self.data['id'] = str(self.target_id)

        # Parse for ROE
        mo = re.search(u'<span>股東權益報酬率</span><[^>]+><[^>]+>([0-9\\.\\-\\,]+)%', self.basic_info_raw)
        if mo == None:
            raise RuntimeError('PriceCrawlingJob: unable to parse ROE value. stock id: ' + str(self.target_id))
        elif len(mo.groups()) != 1:
            raise RuntimeError('PriceCrawlingJob: unexpected number of matched groups for RE of ROE.')
        if mo.group(1) == '-':
            self.data['roe'] = float(0.0)
        else:
            self.data['roe'] = float(mo.group(1).replace(',',''))
    
        # Parse for price
        mo = re.search(u'成交</span><[^>]+>([0-9\\.\\,]+)</span>', self.price_info_raw)
        if mo == None:
            raise RuntimeError('PriceCrawlingJob: unable to parse price value. stock id:' + str(self.target_id))
        elif len(mo.groups()) != 1:
            raise RuntimeError('PriceCrawlingJob: unexpected number of matched groups for RE of price.')
        self.data['price'] = float(mo.group(1).replace(',',''))

        
