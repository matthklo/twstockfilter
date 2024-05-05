#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function
import json
import re
import tempfile
import time
import subprocess

def fetch_dividend_data(y):
  # See if we have local data for the year...
  data_filename = 'dividend_data/year_' + str(y) + '.json'
  try:
    with open(data_filename, 'r') as rf:
      return json.load(rf)
  except:
    pass

  # Local data is not available, fetch from web...
  print('Fetching dividend data for year ' + str(y) + ' ...')
  target_url = 'https://goodinfo.tw/tw/StockDividendPolicyList.asp?MARKET_CAT=%%E4%%B8%%8A%%E5%%B8%%82&INDUSTRY_CAT=%%E5%%85%%A8%%E9%%83%%A8&YEAR=%d' % y

  with tempfile.TemporaryFile() as tmpout:
    # On Ubuntu, install chromium-browser with command: 'apt install chromium-browser'
    chromium_path = '/usr/bin/chromium-browser'

    # Download the full web content with the help of Chromium browser
    subprocess.check_call([chromium_path, '--no-sandbox', '--no-default-browser-check', '--no-first-run',
      '--disable-default-apps', '--disable-popup-blocking', '--disable-translate', '--enable-logging',
      '--disable-background-timer-throttling', '--headless', '--disable-gpu', '--dump-dom', '--virtual-time-budget=10000',
      target_url], stdout=tmpout)
    tmpout.seek(0)

    # save the fetched data to local file for debugging
    #with open('test.txt', 'wb') as wf:
    #  wf.write(tmpout.read())
    #  tmpout.seek(0)

    dividend_data = {}
    cnt = 0
    for entry in re.finditer(r"<tr.+?</tr>", tmpout.read()):  # For each HTML table row ...
      #print(entry.group(0))
      cells = []
      for cell in re.finditer(r"<td.+?</td>", entry.group(0)):  # For each HTML table cell (<td...</td>) ...
        # Strip all html tag (<...>) in the matched string.
        cells.append(re.sub(r"<.+?>", "", cell.group(0)).strip())
        #print(cells[len(cells)-1])

      #print('cells len = ' + str(len(cells)))
      if len(cells) < 16:
        continue
      if cells[0].decode('utf-8') != u'上市':
        continue
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
    if cnt > 0:
      with open(data_filename, 'w') as wf:
        json.dump(dividend_data, wf)

  return dividend_data

def generate_cdys():
  # Collect the dividend data in the time span of 1984 ~ {Year}.
  # Where {Year} is current year if current date has passed June 30th, otherwise 
  # it's the last year.
  curDate = time.localtime()
  curYear = curDate.tm_year
  if curDate.tm_mon <= 6:
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
  #data = fetch_dividend_data(2024)

  cdys = generate_cdys()
  print(str(cdys))
