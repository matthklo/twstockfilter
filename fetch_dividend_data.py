#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function
import json
import re
import urllib2
import time

def fetch_dividend_data(y):
  # See if we have local data for the year...
  data_filename = 'dividend_data/year_' + str(y) + '.json'
  try:
    with open(data_filename, 'r') as rf:
      return json.load(rf)
  except:
    pass

  # Local data is not available, fetch from web...
  target_url = 'https://goodinfo.tw/StockInfo/StockDividendPolicyList.asp?MARKET_CAT=%%E4%%B8%%8A%%E5%%B8%%82&INDUSTRY_CAT=%%E5%%85%%A8%%E9%%83%%A8&YEAR=%d' % y
  req = urllib2.Request(target_url, None, headers = {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36' })
  r = urllib2.urlopen(req)
  if r.getcode() != 200:
    print('Error: Failed to fetch dividend data from Goodinfo.tw. Year: ' + str(y))
    return None

  #with open('test.txt', 'wb') as wf:
  #  wf.write(r.read())

  dividend_data = {}
  cnt = 0
  for entry in re.finditer(r"<tr id='row[0-9]+' .+?</tr>", r.read()):  # For each HTML table row ...
    cells = []
    for cell in re.finditer(r"<td.+?</td>", entry.group(0)):  # For each HTML table cell (<td...</td>) ...
      # Strip all html tag (<...>) in the matched string.
      cells.append(re.sub(r"<.+?>", "", cell.group(0)).strip())
      #print(cells[len(cells)-1])
      
    #print('Cells count = ' + str(len(cells)))

    # As current state (2020 July), there should be 20 cells in an entry. The data we interest are
    # 1: Stock ID
    # 2: Stock Name (in Traditional Chinese)
    # 3,4: Dividend years (delivery year & dividend year)
    # 5: Annual profit (Unit: 100 million NTD)
    # 6: EPS
    # 7,8,9:    Cash dividends (profit, capital, subtotal -- profit + captital)
    # 10,11,12: Stock dividens (profit, captial, subtotal -- profit + captital)
    # 13:       Dividen subtotal (Cell #9 + Cell #12)

    dividend_data[cells[1]] = cells

    cnt+=1
  print('Dividend data has fetched from web. Year: ' + str(y) + ', Stock numbers: ' + str(cnt))

  # Preserve data at local.
  with open(data_filename, 'w') as wf:
    json.dump(dividend_data, wf)

  return dividend_data

def generate_cdys():
  # Collect the dividend data in the time span of 1984 ~ {Year}.
  # Where {Year} is current year if current date has passed June 30th, otherwise 
  # it's the last year.
  curDate = time.localtime()
  curYear = curDate.tm_year
  if curDate.tm_mon < 6:
    curYear -= 1
  
  d = {}
  for y in range(1984, curYear+1):
    d[y] = fetch_dividend_data(y)
  
  cdys = {}
  for stock_id in d[curYear]:
    ycnt = 0
    for test_year in range(curYear, 1983, -1):
      if stock_id not in d[test_year]:
        break
      dividend_subtotal_str = d[test_year][stock_id][13]

      try:
        dividend_subtotal = float(dividend_subtotal_str)
        if dividend_subtotal <= 0.0:
          break
        ycnt += 1
      except:
        break
    cdys[stock_id] = ycnt
  
  return cdys


if __name__ == '__main__':
  #data = fetch_dividend_data(2020)

  cdys = generate_cdys()
  print(str(cdys))