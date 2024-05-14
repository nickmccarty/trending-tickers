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

    url = 'https://feeds.finance.yahoo.com/rss/2.0/headline?s=%s&region=US&lang=en-US'%ticker

    response = requests.get(url, headers=headers)
    feed = feedparser.parse(response.text)

    # Get the most recent story if available
    if feed.entries:
        most_recent_stories = feed.entries
        most_recent_story = most_recent_stories[0]

        # Convert published date string to datetime object
        article_timestamp = datetime.strptime(most_recent_story.published, "%a, %d %b %Y %H:%M:%S %z")
        article_title = most_recent_story.title
        article_link = most_recent_story.link
        article_summary = most_recent_story.summary
    else:
        article_timestamp.append('')
        article_title.append('')
        article_link.append('')
        article_summary.append('')

    return article_timestamp, article_title, article_summary, article_link

def scrape_trending_tickers():
    current_time = datetime.now()
    url = "https://finance.yahoo.com/trending-tickers/"
    
    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'
              }

    # Fetch the webpage content
    response = requests.get(url, headers=headers)
    html_content = response.text

    # Parse HTML
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the table containing the data
    table = soup.find("table")

    # Extract table rows
    rows = table.find_all("tr")

    # Initialize a list to store the parsed data
    parsed_data = []

    # Loop through rows and extract data
    for row in rows:
        # Extract table data cells
        cells = row.find_all("td")
        # Extract text from each cell and add to parsed_data if number of columns is consistent
        # if len(cells) == 11:  # Assuming the table has 11 columns
        parsed_row = [cell.text.strip() for cell in cells]
        parsed_data.append(parsed_row)

    # Extract header row
    header_row = [header.text.strip() for header in rows[0].find_all("th")]

    # Convert parsed data to DataFrame
    df = pd.DataFrame(parsed_data, columns=header_row)

    # Display DataFrame
    trending_tickers_df = df.drop(['Intraday High/Low', '52 Week Range', 'Day Chart'], axis = 1).dropna()
    current_time = str(current_time)
    market_time = trending_tickers_df['Market Time'].tolist()
    ticker_symbols = trending_tickers_df['Symbol'].tolist()
    company_names = trending_tickers_df['Name'].tolist()
    last_price = trending_tickers_df['Last Price'].tolist()
    percent_changes = trending_tickers_df['% Change'].astype(str).str.lstrip('+').str.replace(',', '').str.rstrip('%').astype(float).tolist()
    trading_volume = trending_tickers_df['Volume'].astype(str).str.strip().tolist()
    market_cap = trending_tickers_df['Market Cap'].astype(str).str.strip().tolist()

    sector = []
    industry = []

    for ticker in ticker_symbols:

      try:
            ticker_data = yf.Ticker(ticker)
            sector.append(ticker_data.info['sector'])
            industry.append(ticker_data.info['industry'])
      except Exception as e:
          # print("Error occurred while processing ticker", ticker, ":", e)
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
            # print("Error occurred while processing ticker", ticker, ":", e)
            article_timestamp.append('')
            article_title.append('')
            article_link.append('')
            article_summary.append('')

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
