import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timezone
import json
import yfinance as yf

def _parse_news_timestamp(value):
    """Parse a yfinance news pubDate/displayTime (ISO 8601, e.g. '2026-06-24T15:55:32Z')
    into a timezone-aware datetime, matching the archive timestamp format."""
    if not value:
        return None
    try:
        # fromisoformat handles offsets; normalise a trailing 'Z' to +00:00
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        return None

def get_recent_news(ticker, ticker_data=None):
    """Return (timestamp, title, summary, link) for the most recent news story
    about ``ticker``.

    The legacy Yahoo RSS endpoint (feeds.finance.yahoo.com/rss/2.0/headline) was
    retired in 2025 and now returns HTTP 429 for every request, which is why news
    coverage dropped to 0%. yfinance exposes the same Yahoo Finance news via
    ``Ticker.news``, so we use that instead. A ``ticker_data`` object can be passed
    in to reuse an existing yf.Ticker and avoid a duplicate lookup.
    """
    if ticker_data is None:
        ticker_data = yf.Ticker(ticker)

    try:
        items = ticker_data.news or []
    except Exception:
        items = []

    stories = []
    for item in items:
        # Newer yfinance nests the fields under "content"; older versions are flat.
        content = item.get('content', item) if isinstance(item, dict) else {}
        title = content.get('title') or ''
        if not title:
            continue
        ts = _parse_news_timestamp(content.get('pubDate') or content.get('displayTime'))
        summary = content.get('summary') or content.get('description') or ''
        link = (content.get('canonicalUrl') or {}).get('url') \
            or (content.get('clickThroughUrl') or {}).get('url') \
            or content.get('link') or ''
        stories.append((ts, title, summary, link))

    if not stories:
        return None, '', '', ''

    # Most recent story first (items without a timestamp sort last).
    stories.sort(key=lambda s: s[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return stories[0]

def scrape_trending_tickers():
    current_time = datetime.now()
    url = "https://finance.yahoo.com/markets/stocks/trending/"
    # url = "https://finance.yahoo.com/markets/stocks/most-active/?start=0&count=200"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    # Fetch the webpage
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the trending stocks table
    table = soup.find('table', {'class': 'bd'})
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
        if column in df.columns:
            df[column] = pd.to_numeric(df[column]
                                      .str.replace('M', 'e6', regex=False)
                                      .str.replace('B', 'e9', regex=False)
                                      .str.replace('%', '', regex=False),
                                      errors='coerce')
        else:
            df[column] = ''  # Add a column with NaNs if it doesn't exist
    
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
    article_timestamp = []
    article_title = []
    article_summary = []
    article_link = []
    for ticker in ticker_symbols:
        try:
            ticker_data = yf.Ticker(ticker)
            sector.append(ticker_data.info.get('sector', ''))
            industry.append(ticker_data.info.get('industry', ''))
        except Exception as e:
            ticker_data = None
            sector.append('')
            industry.append('')

        try:
            timestamp, title, summary, link = get_recent_news(ticker, ticker_data)
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
