import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
import csv
import os
import nltk
from datetime import datetime
from itemadapter import ItemAdapter
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import time
import logging
import requests
import schedule
from multiprocessing import Process
import random

"""
Market Analysis Terminal - Data Acquisition Pipeline.

This module implements a hybrid ETL (Extract, Transform, Load) system for financial markets.
It combines REST API integration for high-fidelity historical data with targeted HTML scraping
for real-time sentiment and commodity prices.

Key Components:
1. Historical Backfill (Alpha Vantage API):
   - Fetches 100 days of OHLCV data to establish a baseline.
   - Handles API rate limiting and key validation.

2. Real-Time Spider (Scrapy):
   - Targeted scraping of Finviz, MarketWatch, Macrotrends, and EIA.
   - Features robust anti-detection mechanisms (User-Agent rotation, random delays).
   - Performs on-the-fly Sentiment Analysis (VADER) on news headlines.

3. Persistence Layer (HDF5):
   - Unifies diverse data sources (API JSON, HTML Tables, Unstructured Text) into a single
     hierarchical HDF5 store ('market_data.h5') for efficient I/O.
"""

# Configure logging to capture critical errors and warnings
logging.basicConfig(
    filename='scraper_errors.log', 
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- CONFIGURATION ---
HDF5_PATH = "market_data.h5"
CSV_PATH = "targets.csv"
SECRETS_PATH = "secrets.csv"

# List of legitimate Browser User-Agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15'
]

def get_config(file_path=SECRETS_PATH):
    """
    Reads configuration secrets from a CSV file.

    Args:
        file_path (str): Path to the secrets CSV file. Defaults to SECRETS_PATH.

    Returns:
        dict: A dictionary containing key-value pairs from the secrets file.
              Returns an empty dict if the file is not found or cannot be read.
    """
    config = {}
    try:
        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if 'key' in row and 'value' in row:
                    config[row['key']] = row['value'].strip()
        return config
    except FileNotFoundError:
        print("⚠️ secrets.csv not found. Please create it with api_key and email.")
        return {}

# Load secrets securely
secrets = get_config()
contact_email = secrets.get('email', 'email@test.org')
API_KEY = secrets.get('api_key', '')

if not API_KEY:
    logging.error("API Key not found in secrets.csv! Historical backfill will fail.")

# Initialize Sentiment Analyzer
try:
    sia = SentimentIntensityAnalyzer()
except LookupError:
    nltk.download('vader_lexicon')
    sia = SentimentIntensityAnalyzer()

# --- PART 1: ALPHA VANTAGE BACKFILL (HDF5) ---
def backfill_history():
    """
    Performs a historical data backfill using the Alpha Vantage API.

    This function acts as a pre-scraping step to ensure the dashboard is populated
    with at least 100 days of historical candlestick data. 
    
    Data Model Reasoning:
    Stores data in HDF5 under the key 'history'. This format is efficient in 
    appending and querying time-series data without loading the entire file.

    Returns:
        None. Saves data directly to 'market_data.h5'.
    """
    if not API_KEY:
        print("❌ Backfill skipped: No API Key.")
        return

    print("📉 Starting Historical Backfill...")
    tickers_to_fetch = []
    
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['type'].strip() == 'history_alpha':
                    tickers_to_fetch.append(row['ticker'].strip())
    
    if not tickers_to_fetch:
        print(f"⚠️ No 'history_alpha' tickers found in {CSV_PATH}.")
        return

    all_data = []
    
    for ticker in tickers_to_fetch:
        print(f"   ⏳ Fetching history for {ticker}...")
        try:
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&outputsize=compact&apikey={API_KEY}"
            r = requests.get(url)
            data = r.json()
            
            if "Note" in data:
                print("   ⚠️ API Limit. Waiting 60s...")
                time.sleep(65)
                r = requests.get(url)
                data = r.json()

            ts = data.get("Time Series (Daily)", {})
            if ts:
                df = pd.DataFrame.from_dict(ts, orient='index')
                df = df.reset_index().rename(columns={'index': 'date'})
                df = df.rename(columns={
                    '1. open': 'open', '2. high': 'high', '3. low': 'low', 
                    '4. close': 'close', '5. volume': 'volume'
                })
                for c in ['open', 'high', 'low', 'close', 'volume']:
                    df[c] = pd.to_numeric(df[c])
                
                df['ticker'] = ticker
                df['date'] = pd.to_datetime(df['date'])
                df['type'] = 'History'
                
                all_data.append(df)
                print(f"   ✅ {ticker}: {len(df)} days.")
            else:
                print(f"   ❌ {ticker}: No data found.")
            
            time.sleep(12) 

        except Exception as e:
            print(f"   ❌ Error {ticker}: {e}")

    if all_data:
        new_df = pd.concat(all_data)
        try:
            with pd.HDFStore(HDF5_PATH, mode='a') as store:
                key = 'history'
                if f'/{key}' in store.keys():
                    existing = store[key]
                    combined = pd.concat([existing, new_df])
                    combined = combined.drop_duplicates(subset=['date', 'ticker'], keep='last')
                    store.put(key, combined, format='table', data_columns=True)
                else:
                    store.put(key, new_df, format='table', data_columns=True)
            print(f"✅ History saved to {HDF5_PATH}")
        except Exception as e:
            print(f"❌ HDF5 Save Error: {e}")

# --- PART 2: DAILY API FETCH ---
def fetch_daily_api():
    """
    Execute the Historical Backfill process.
    
    Iterates through tickers defined in 'targets.csv' (type: history_alpha),
    fetches 100 days of OHLCV data via Alpha Vantage, and saves it to HDF5.
    
    Note:
        This function is blocking and runs before the spider starts to ensure
        the dashboard has a baseline dataset.
    """
    if not API_KEY:
        print("❌ API Key missing. Cannot fetch daily prices.")
        return

    print("💰 Fetching Daily OHLC via API (Alpha Vantage)...")
    tickers = set()
    
    # 1. Read tickers from targets.csv
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # --- [RESTORED LOGIC] ---
                # We only want rows marked as 'stock_finviz' (Stocks).
                # This includes TTE (TotalEnergies) which we added as a proxy.
                if row['type'].strip() == 'stock_finviz' and row['ticker'].strip():
                    tickers.add(row['ticker'].strip())

    if not tickers:
        print("⚠️ No stock tickers found in CSV to fetch.")
        return

    api_data = []
    for ticker in tickers:
        try:
            # Request Global Quote (Real-time daily data)
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={API_KEY}"
            r = requests.get(url)
            data = r.json()
            
            # Handle Rate Limits
            if "Note" in data:
                print(f"   ⚠️ Rate Limit on {ticker}. Waiting 60s...")
                time.sleep(60)
                r = requests.get(url)
                data = r.json()

            quote = data.get("Global Quote", {})
            if quote:
                api_data.append({
                    'date': datetime.now(),
                    'ticker': ticker,
                    'open': float(quote.get('02. open', 0)),
                    'high': float(quote.get('03. high', 0)),
                    'low': float(quote.get('04. low', 0)),
                    'close': float(quote.get('05. price', 0)),
                    'volume': int(quote.get('06. volume', 0)),
                    'type': 'Stock'
                })
                print(f"   ✅ {ticker} API: ${quote.get('05. price')}")
            else:
                print(f"   ❌ {ticker}: No quote data found.")
            
            time.sleep(12) # Safety delay

        except Exception as e:
            print(f"   ❌ API Error {ticker}: {e}")

    # 2. Save Data to HDF5
    if api_data:
        new_df = pd.DataFrame(api_data)
        new_df['date'] = pd.to_datetime(new_df['date'])
        
        try:
            with pd.HDFStore(HDF5_PATH, mode='a') as store:
                key = 'stocks'
                if f'/{key}' in store.keys():
                    existing = store[key]
                    combined = pd.concat([existing, new_df])
                    # Deduplicate: Keep latest for today
                    combined = combined.drop_duplicates(subset=['date', 'ticker'], keep='last')
                    store.put(key, combined, format='table', data_columns=True)
                else:
                    store.put(key, new_df, format='table', data_columns=True)
            print("✅ API Data Saved to HDF5.")
        except Exception as e:
            print(f"❌ Save Error: {e}")

# --- PART 3: THE SPIDER (HTML SCRAPING) ---
class HDF5Pipeline:
    

    def open_spider(self, spider):
        self.stock_items = []
        self.news_items = []
        self.macro_items = []
        self.fund_items = []

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        item_type = item.get('type')

        if item_type in ['Stock', 'stock_finviz']:
            self.stock_items.append(adapter.asdict())
        elif item_type in ['News', 'news_finviz', 'news_marketwatch', 'news_energy']:
            self.news_items.append(adapter.asdict())
        elif item_type in ['commodity']: 
            self.macro_items.append(adapter.asdict())
        elif item_type in ['Fundamental', 'fund_macrotrends']:
            self.fund_items.append(adapter.asdict())
        return item

    def close_spider(self, spider):
        self.save_hdf5(pd.DataFrame(self.stock_items), 'stocks', ['date', 'ticker'])
        self.save_hdf5(pd.DataFrame(self.news_items), 'news', ['date', 'ticker', 'title'])
        self.save_hdf5(pd.DataFrame(self.macro_items), 'macro', ['date', 'ticker'])
        self.save_hdf5(pd.DataFrame(self.fund_items), 'fundamentals', ['date', 'ticker'])

    def save_hdf5(self, new_df, key, distinct_cols):
        if new_df.empty: return
        
        if 'date' in new_df.columns:
            new_df['date'] = pd.to_datetime(new_df['date'], utc=True).dt.tz_localize(None)
        
        for col in new_df.select_dtypes(include=['object']).columns:
            new_df[col] = new_df[col].astype(str)

        try:
            with pd.HDFStore(HDF5_PATH, mode='a') as store:
                if f'/{key}' in store.keys():
                    existing = store[key]
                    combined = pd.concat([existing, new_df])
                    if set(distinct_cols).issubset(combined.columns):
                        combined = combined.drop_duplicates(subset=distinct_cols, keep='last')
                    store.put(key, combined, format='table', data_columns=True)
                else:
                    store.put(key, new_df, format='table', data_columns=True)
            print(f"✅ Saved {len(new_df)} rows to '/{key}'")
        except Exception as e:
            logging.error(f"Save failed {key}: {e}")

class UniversalSpider(scrapy.Spider):
    """
    A unified Scrapy Spider for handling multiple financial domains.

    Attributes:
        name (str): 'market_spider'
        custom_settings (dict): Configures download delays, user agents, and headers.
    """
    name = "universal_spider"
    
    custom_settings = {
        'ITEM_PIPELINES': {'__main__.HDF5Pipeline': 300},
        'DOWNLOAD_DELAY': 5,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'COOKIES_ENABLED': False
    }

    def start_requests(self):
        """
        Generator that reads 'targets.csv' and yields Requests for each URL.
        Attaches metadata (ticker, type) to the Request object for use in callbacks.
        """
        if not os.path.exists(CSV_PATH): return
        with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Standard logic: Skip stocks (API handles them) and History (Backfill handles it)
                if not row['url'].strip() or row['type'] in ['history_alpha', 'stock_finviz']: 
                    continue
                
                yield scrapy.Request(
                    url=row['url'].strip(),
                    callback=self.parse,
                    meta={'type': row['type'], 'ticker': row['ticker']},
                    dont_filter=True,
                    headers={'User-Agent': random.choice(USER_AGENTS)}
                )

    def parse(self, response):
        """
        Main routing function. Dispatches the response to specific parsing logic
        based on the 'type' field defined in targets.csv.
        """
        data_type = response.meta.get('type')
        ticker = response.meta.get('ticker')
        
        try:
            # === 1. STOCK PRICES (OHLC) ===
            """
        Parses real-time stock prices from Finviz HTML tables.
        Target: Finviz Quote Page
        """
            if data_type == 'stock_finviz':
                # Get Current Price
                current_str = response.css('div.quote-header_ticker-wrapper_price::text').get()
                if not current_str:
                     current_str = response.xpath('//td[contains(text(), "Price")]/following-sibling::td[1]/b/text()').get()
                
                # Get Open/High/Low from table
                open_v = response.xpath('//td[contains(text(), "Open")]/following-sibling::td[1]/b/text()').get()
                high_v = response.xpath('//td[contains(text(), "High")]/following-sibling::td[1]/b/text()').get()
                low_v = response.xpath('//td[contains(text(), "Low")]/following-sibling::td[1]/b/text()').get()

                def clean(p):
                    if not p: return 0.0
                    return float(p.replace(',', '')) if p.replace('.', '', 1).isdigit() else 0.0

                current = clean(current_str)
                o = clean(open_v) if open_v else current
                h = clean(high_v) if high_v else current
                l = clean(low_v) if low_v else current

                if current > 0:
                    yield {
                        'type': 'Stock', 'ticker': ticker, 'date': datetime.now(),
                        'open': o, 'high': h, 'low': l, 'close': current, 'volume': 0 
                    }
                    print(f"💰 {ticker}: {current}")

            # === 2. ENERGY NEWS ===
            elif data_type == 'news_energy':
                headlines = response.css('div.categoryArticle__content h2::text').getall()
                if not headlines:
                    headlines = response.css('div.singleArticle__content h3::text').getall()
                
                found = 0
                for title in headlines[:5]:
                    title = title.strip()
                    if title:
                        yield {
                            'type': 'News', 'ticker': 'ENERGY', 'title': title,
                            'date': datetime.now(),
                            'sentiment': sia.polarity_scores(title)['compound'],
                            'source': 'OilPrice'
                        }
                        found += 1
                if found > 0: print(f"⚡ Energy: {found} articles")

            # === 3. GENERAL NEWS (Finviz) ===
            # Parse news from Finwiz, then from marketwatch and Macrotrends Data
            elif data_type == 'news_finviz':
                rows = response.css('table#news-table tr')
                count = 0
                for row in rows:
                    if count >= 3: break
                    headline = row.css('a.tab-link-news::text').get()
                    if headline:
                        yield {
                            'type': 'News', 'ticker': ticker, 'title': headline.strip(),
                            'date': datetime.now(),
                            'sentiment': sia.polarity_scores(headline)['compound'],
                            'source': 'Finviz'
                        }
                        count += 1

            # === 4. GENERAL NEWS (MarketWatch) ===
            elif data_type == 'news_marketwatch':
                articles = response.css('div.collection__elements div.element--article')
                count = 0
                for art in articles:
                    if count >= 3: break
                    headline = art.css('a.link::text').get()
                    if headline:
                        yield {
                            'type': 'News', 'ticker': ticker, 'title': headline.strip(),
                            'date': datetime.now(),
                            'sentiment': sia.polarity_scores(headline)['compound'],
                            'source': 'MarketWatch'
                        }
                        count += 1

            # === 5. FUNDAMENTALS (Macrotrends) ===
            elif data_type == 'fund_macrotrends':
                val_str = response.css('table.historical_data_table tbody tr:first-child td:nth-child(4)::text').get()
                if val_str:
                    yield {
                        'type': 'Fundamental', 'ticker': ticker, 'date': datetime.now(),
                        'pe_ratio': val_str,
                        'source': 'Macrotrends'
                    }

            # === 6. COMMODITIES ===
            # parse function for Commodities
            elif data_type == 'commodity':
                prices = response.css('td.value::text').getall()
                if prices:
                    yield {'type': 'commodity', 'ticker': ticker, 'date': datetime.now(), 'close': prices[0].strip()}

        except Exception as e:
            logging.error(f"Parse Error {ticker}: {e}")

# --- PART 4: SCHEDULING & EXECUTION ---
def run_crawl_process():
    """
    Instantiates and runs the CrawlerProcess in a standalone function.
    Target function for multiprocessing to ensure a clean Reactor state.
    """
    print(f"\n🚀 Starting Spider at {datetime.now().strftime('%H:%M:%S')}...")
    process = CrawlerProcess()
    process.crawl(UniversalSpider)
    process.start()

def job():
    """
    Main Execution Job:
    1. Executes Historical Backfill (Blocking call, ensures base data integrity).
    2. Launches the Spider in a separate Process (Required for Scrapy re-execution).
    """
    # Step 1: Backfill History (HDF5)
    print(f"\n🚀 DAILY RUN: {datetime.now()}")
    
    # 1. Fetch Accurate Prices (API)
    fetch_daily_api()
    
    # 2. Scrape News & Commodities (Spider)
    # It runs the spider in a separate process to avoid Reactor errors
    p = Process(target=run_crawl_process)
    p.start()
    p.join()
    
    print("✅ Daily Job Complete.")

if __name__ == "__main__":
    print(f"=== MARKET SPIDER with Hybrid HDF5 STARTED ===")
    
    # Execute immediately on startup
    job() 

    # Process Automatization: Schedule daily execution at 6:00 PM
    print("\n2. Scheduling daily scan for 6:00 PM...")
    schedule.every().day.at("18:00").do(job)
    
    # Keep the main process alive to maintain the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)