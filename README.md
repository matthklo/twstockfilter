# twstockfilter
從奇摩股票、以及 Goodinfo! 這兩個網站為來源，將台灣上市股票資訊抓下來儲存到 Google Cloud Datastore 中 (有免費使用額度)。並且供其它網頁服務使用。

## 環境設定
- 可在 Windows / Unix 上執行
- 需求 Python 2.7.x (3.x 沒測試過)
- 需求以 pip 安裝 google-cloud-datastore 套件

    pip install --upgrade google-cloud-datastore
    
- 需求一個 Google 帳號，到 Google API Console 中建立服務帳戶並且下載其金鑰 (JSON 檔案)，需開通 Datastore API


## 架構

### WorkPool.py
可建立指定數量的 thread pool，同時併發多個交付的 job 來執行。

### PriceCrawlingJob.py 以及 DividendCrawlingJob.py 
分別向奇摩股票、以及 Goodinfo! 這兩個網站抓取資料的 "job"。

### update_dividend_data.py 
建立 WorkPool，產生 DividendCrawlingJob 交付執行，目的是提取台灣所有上市股票的歷年配息配股數據統整後上傳到 Cloud Datastore 中儲存。

### update_price_data.py 
建立 WorkPool，產生 PriceCrawlingJob 交付執行，目的是提取所有上市股票的最新成交價格，統整後上傳到 Cloud Datastore 中儲存。

### query.py
方便於 console 中驗證 Datastore 中的資料用的。可向 Datastore 發出 query 篩選近五年平均殖利高於指定值以上者，作為定期存股的目標。


## 使用

建議配合 cron 半年跑一次 update_dividend_data.py、每天跑一次 update_price_data.py。
即可隨時使用 query.py 來查詢。

使用前都需要先設定環境變數 GOOGLE_APPLICATION_CREDENTIALS，內容是你的 Google API 服務帳戶金鑰 JSON 檔案的路徑。