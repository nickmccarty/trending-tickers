import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import json
import feedparser
import yfinance as yf

def get_recent_news(ticker):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'
    }

    url = 'https://feeds.finance.yahoo.com/rss/2.0/headline?s=%s&region=US&lang=en-US' % ticker
    response = requests.get(url, headers=headers)
    feed = feedparser.parse(response.text)

    if feed.entries:
        most_recent_story = feed.entries[0]
        article_timestamp = datetime.strptime(most_recent_story.published, "%a, %d %b %Y %H:%M:%S %z")
        article_title = most_recent_story.title
        article_link = most_recent_story.link
        article_summary = most_recent_story.summary
    else:
        article_timestamp = None
        article_title = ''
        article_link = ''
        article_summary = ''

    return article_timestamp, article_title, article_summary, article_link

def scrape_trending_tickers():
    # Get current time
    current_time = datetime.now()

    # URL and headers
    url = "https://finance.yahoo.com/markets/stocks/trending/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # Fetch the webpage
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the trending stocks table
    table = soup.find('table', {'class': 'markets-table'})
    if table is None:
        raise ValueError("Trending stocks table not found")

    # Extract table headers
    headers = [th.get_text(strip=True) for th in table.find_all('th')]

    # Extract table rows
    rows = []
    for tr in table.find_all('tr')[1:]:  # Skip header row
        cells = tr.find_all('td')
        if cells:
            row = [cell.get_text(strip=True) for cell in cells]
            rows.append(row)

    # Create DataFrame
    df = pd.DataFrame(rows, columns=headers)

    # Clean numeric columns
    for col in ['Price', 'Change', 'Change %']:
        if col in df.columns:
            df[col] = df[col].str.replace('[%,]', '', regex=True).str.replace('—', '0')
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert volume and market cap
    def convert_to_number(val):
        if isinstance(val, str):
            if 'M' in val:
                return float(val.replace('M', '')) * 1e6
            elif 'B' in val:
                return float(val.replace('B', '')) * 1e9
            elif 'K' in val:
                return float(val.replace('K', '')) * 1e3
        return pd.to_numeric(val, errors='coerce')

    for col in ['Volume', 'Avg Vol (3M)', 'Market Cap']:
        if col in df.columns:
            df[col] = df[col].apply(convert_to_number)

    # Add market time as None
    market_time = [None] * len(df)

    # Fetch sector and industry from yfinance
    sectors, industries = [], []
    for symbol in df['Symbol']:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.history(period="1d")
            sectors.append(ticker.info.get('sector', '') if hasattr(ticker, "info") else '')
            industries.append(ticker.info.get('industry', '') if hasattr(ticker, "info") else '')
        except Exception:
            sectors.append('')
            industries.append('')

    # Placeholder for article data (since get_recent_news is missing)
    article_timestamp = [None] * len(df)
    article_title = [''] * len(df)
    article_summary = [''] * len(df)
    article_link = [''] * len(df)

    return (
        current_time,
        market_time,
        df['Symbol'].tolist(),
        df['Name'].tolist(),
        sectors,
        industries,
        df['Price'].tolist(),
        df['Change %'].tolist(),
        df['Volume'].tolist(),
        df['Market Cap'].tolist(),
        article_timestamp,
        article_title,
        article_summary,
        article_link
    )

def save_to_sqlite(current_time, market_time, ticker_symbols, company_names, sector, industry, last_price, percent_changes, trading_volume, market_cap, article_timestamp, article_title, article_summary, article_link):
    db_file = 'trending-tickers.db'
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # Create the table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS trending_tickers
                 (utc_timestamp TEXT, market_time TEXT, ticker_symbol TEXT, company_name TEXT, sector TEXT, industry TEXT, last_price REAL, percent_change REAL, trading_volume TEXT, market_cap TEXT, article_timestamp TEXT, article_title TEXT, article_summary TEXT, article_link TEXT)''')

    # Insert the data
    for market_time, ticker_symbol, company_name, sector, industry, last_price, percent_change, trading_volume, market_cap, article_timestamp, article_title, article_summary, article_link in zip(market_time, ticker_symbols, company_names, sector, industry, last_price, percent_changes, trading_volume, market_cap, article_timestamp, article_title, article_summary, article_link):

        c.execute("INSERT INTO trending_tickers (utc_timestamp, market_time, ticker_symbol, company_name, sector, industry, last_price, percent_change, trading_volume, market_cap, article_timestamp, article_title, article_summary, article_link) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (current_time, market_time, ticker_symbol, company_name, sector, industry, last_price, percent_change, trading_volume, market_cap, article_timestamp, article_title, article_summary, article_link))

    conn.commit()
    conn.close()

def render_html(current_time, market_time, tickers, names, last_price, percent_changes, volume, market_cap):
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("index.html.j2")
    html = template.render(current_time=current_time, market_time=market_time, tickers=tickers, names=names, last_price=last_price, percent_changes=percent_changes, volume=volume, market_cap=market_cap)

    with open("index.html", "w") as f:
        f.write(html)

if __name__ == "__main__":
    current_time, market_time, ticker_symbols, company_names, sector, industry, last_price, percent_changes, trading_volume, market_cap, article_timestamp, article_title, article_summary, article_link = scrape_trending_tickers()
    save_to_sqlite(current_time, market_time, ticker_symbols, company_names, sector, industry, last_price, percent_changes, trading_volume, market_cap, article_timestamp, article_title, article_summary, article_link)
    render_html(current_time, market_time, ticker_symbols, company_names, last_price, percent_changes, trading_volume, market_cap)
