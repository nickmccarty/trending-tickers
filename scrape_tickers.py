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
    current_time = datetime.now()
    # url = "https://finance.yahoo.com/markets/stocks/trending/"
    url = "https://finance.yahoo.com/markets/stocks/most-active/?start=0&count=200"
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
    data = []
    for row in table.find_all('tr')[1:]:  # Skip the header row
        cells = row.find_all('td')
        if cells:
            row_data = []
            for cell in cells:
                link = cell.find('a', {'data-testid': 'table-cell-ticker'})
                if link:
                    row_data.append(link.get_text(strip=True))  # Ticker symbol
                else:
                    row_data.append(cell.get_text(strip=True))  # Other cell values
            data.append(row_data)

    # Create a DataFrame
    df = pd.DataFrame(data, columns=headers)

    # Clean the Price, Change, and Change % columns
    df[['Price', 'Change', 'Change %']] = df['Price'].str.extract(r'(\d+\.\d+)([+-]\d+\.\d+)\s*\(([-+]?\d+\.\d+%)\)')

    # Convert data types
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Change'] = pd.to_numeric(df['Change'], errors='coerce')
    df['Change %'] = df['Change %'].str.replace('%', '').astype(float)

    # Optionally, convert other numeric columns if needed
    numeric_columns = ['Volume', 'Avg Vol (3M)', 'Market Cap', 'P/E Ratio (TTM)', '52 Wk Change %']
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column].str.replace('M', 'e6').str.replace('B', 'e9'), errors='coerce')

    # Prepare the data to match the existing schema
    ticker_symbols = df['Symbol'].tolist()
    company_names = df['Name'].tolist()
    last_price = df['Price'].tolist()
    percent_changes = df['Change %'].tolist()
    trading_volume = df['Volume'].tolist()
    market_cap = df['Market Cap'].tolist()

    # For the missing Market Time, we'll pass None
    market_time = [None] * len(ticker_symbols)

    sector = []
    industry = []
    for ticker in ticker_symbols:
        try:
            ticker_data = yf.Ticker(ticker)
            sector.append(ticker_data.info.get('sector', ''))
            industry.append(ticker_data.info.get('industry', ''))
        except Exception as e:
            sector.append('')
            industry.append('')

    article_timestamp = []
    article_title = []
    article_summary = []
    article_link = []
    for ticker in ticker_symbols:
        try:
            timestamp, title, summary, link = get_recent_news(ticker)
            article_timestamp.append(timestamp)
            article_title.append(title)
            article_summary.append(summary)
            article_link.append(link)
        except Exception as e:
            article_timestamp.append(None)
            article_title.append('')
            article_summary.append('')
            article_link.append('')

    return current_time, market_time, ticker_symbols, company_names, sector, industry, last_price, percent_changes, trading_volume, market_cap, article_timestamp, article_title, article_summary, article_link

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
