# Stock Prices Market Analysis and News Terminal with Automated Web Scraping

**Author:** Iman Jouhar  
**Course:** Data Quality and Data Wrangling (DLBDSDQDW01)
**Date:** January 13, 2026

## Overview

This project implements an automated "Market Analysis Terminal" designed to transform manual data collection into a robust, time-series-driven asset. Addressing the "Timeliness" and "Completeness" dimensions of Data Quality, this pipeline automates the extraction of financial data, news sentiment, and commodity prices.

The system stores historical data in a hierarchical format (HDF5), enabling for time series analysis of market trends.


## Features

* **Asynchronous Scraping:** Built on **Scrapy**, utilizing non-blocking I/O for high-velocity data collection.
* **Data Persistence:** Uses **HDF5** storage for high-performance I/O and efficient handling of hierarchical data (Stocks/News).
* **Sentiment Analysis:** Integrates **VADER** (Valence Aware Dictionary and Sentiment Reasoner) to quantify unstructured news headlines into sentiment scores.
* **Automated Scheduling:** Runs as a daemon process, executing data collection daily at 18:00 to ensure data currency.
* **Interactive Dashboard:** A **Stremlit** web application correlates stock prices, trading volume, and news sentiment in real-time.

## Project Structure

* `spider.py`: The core ETL script. It fetches data from APIs and web pages, cleans it, calculates sentiment, and stores it in HDF5 format.
* `stream.py`: The visualization script. It reads the HDF5 file and launches a local web server to display the data.
* `targets.csv`: Configuration input file containing the URLs and tickers to monitor.
* `market_data_spider.h5`: The generated database file (created automatically upon first run).

## Prerequisites

* Python 3.8+
* Pip package manager

## Installation

1.  **Clone or download the repository.**
2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *Note: The `tables` library requires HDF5. If installation fails on Windows, try `pip install tables --only-binary=tables`.*

3.  **VADER Lexicon:**
     Download the VADER lexicon before the first run to avoid errors.

## Configuration

Before running the spider, you must create a `targets.csv` file in the root directory. This file directs the spider on what to scrape.

**Format of `targets.csv`:**
The file must contain headers: `url,type,ticker`.

| Header | Description |
| :--- | :--- |
| **url** | The endpoint URL (API or Webpage). |
| **type** | Must be one of: `stock_api`, `news_api`, or `commodity`. |
| **ticker** | The symbol for the asset (e.g., NVDA, AAPL). |

**Example `targets.csv` content:**
```csv
url,type,ticker, notes
https://www.alphavantage.co/,history_alpha,NKE,Nike History (AlphaVantage)
https://www.alphavantage.co/,history_alpha,NVDA,NVIDIA History (AlphaVantage)
https://www.alphavantage.co/,history_alpha,AAPL,Apple History (AlphaVantage)
https://www.alphavantage.co/,history_alpha,NEE,NextEra History (AlphaVantage)
https://www.alphavantage.co/,history_alpha,SPY,S&P 500 History (AlphaVantage)
https://www.eia.gov/petroleum/gasdiesel/,commodity,GAS_US](https://www.eia.gov/petroleum/gasdiesel/,commodity,GAS_US)
https://finviz.com/quote.ashx?t=NVDA&p=d,html_news,NVDA
https://oilprice.com/Latest-Energy-News/World-News/,energy_news,GAS_DIESEL
https://www.marketwatch.com/investing/stock/nke?mod=search_symbol,news_marketwatch,NKE,Nike News (Source 2)

## Usage

### 1. Data Collection (The Spider)
To start the automated data pipeline:

```bash
python spider.py

Behavior: The script will verify the CSV and enter a waiting loop. It is scheduled to run daily at 18:00 system time.

Immediate Test: To force a run immediately for testing, modify line 164 in spider.py to process(target=run_spider).start() (removing the schedule wrapper) or wait for the scheduled time.

2. Visualization (The Dashboard)
To view the collected data:

Bash

python stream.py
Open your web browser and navigate to: http://127.0.0.1:8051

The dashboard auto-updates every 60 seconds.

Legal & Ethical Considerations
This scraper adheres to ethical web scraping guidelines:

Identification: The bot identifies itself via a custom User-Agent: 'StudentResearchBot/1.0 (Educational Project; contact: replace with your email)'.

Rate Limiting: A download delay is enforced to prevent Denial of Service (DoS) behavior.

Public Data: The project targets publicly accessible data.

## Future Work: The Evolution of Intelligent Scraping

While this project implements a robust standard spider, future iterations would integrate AI to address the inherent limitations of static CSS selectors:

* **Resilient Parsing (AI/LLM Integration):**
    Currently, the scraper relies on specific CSS selectors (e.g., `price = response.css('.current-price')`), which breaks if the website changes its layout ("bit rot"). The next evolutionary step is to implement **LLM-based parsing** or computer vision. This would allow the spider to semantically "read" the page and identify price data regardless of the underlying HTML structure. In alternative, it is possible to use only the api sources that do not have this issue.

* **Advanced NLP (Transformers over Lexicons):**
    The current implementation uses VADER, a lexicon-based model. To improve the "feature engineering" aspect of the pipeline, this should be evolved to use personalized models specifically fine tuned and tested with **Transformer-based models (like FinBERT)**. Finbert and Bert models are more tailored to financial context and nuance reading longer text.