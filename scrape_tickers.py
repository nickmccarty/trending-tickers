import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader
from datetime import datetime
import json
import feedparser

def scrape_trending_tickers():

    # Get the current time
    current_time = datetime.now()

    url = "https://finance.yahoo.com/trending-tickers/"
    
    # Read HTML table from the webpage
    tables = pd.read_html(url)
    
    # Assuming the first table contains the trending tickers
    if tables:
        trending_tickers_df = tables[0]
    else:
        print("No tables found on the webpage.")
        return None

    # Assuming the ticker symbols are in the 'Symbol' column and percent changes are in the '% Change' column
    current_time=str(current_time)
    tickers = trending_tickers_df['Symbol'].tolist()
    names = trending_tickers_df['Name'].tolist()
    last_price = trending_tickers_df['Last Price'].tolist()
    percent_changes = trending_tickers_df['% Change'].str.rstrip('%').astype(float).tolist()
    volume = trending_tickers_df['Volume'].str.strip().tolist()
    market_cap = trending_tickers_df['Market Cap'].str.strip().tolist()

    return current_time, tickers, names, last_price, percent_changes, volume, market_cap

def save_to_sqlite(current_time, tickers, names, last_price, percent_changes, volume, market_cap):
    db_file = 'tickers.db'
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # Create the table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS trending_tickers
                 (current_time TEXT, ticker TEXT, name TEXT, last_price REAL, percent_change REAL, volume TEXT, market_cap TEXT)''')

    # Insert the data
    for ticker, name, last_price, percent_change, volume, market_cap in zip(tickers, names, last_price, percent_changes, volume, market_cap):
        c.execute("INSERT INTO trending_tickers (current_time, ticker, name, last_price, percent_change, volume, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?)", (current_time, ticker, name, last_price, percent_change, volume, market_cap))

    conn.commit()
    conn.close()

def render_html(current_time, tickers, names, last_price, percent_changes, volume, market_cap):
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("index.html.j2")
    html = template.render(current_time=current_time, tickers=tickers, names=names, last_price=last_price, percent_changes=percent_changes, volume=volume, market_cap=market_cap)

    with open("index.html", "w") as f:
        f.write(html)

if __name__ == "__main__":
    current_time, tickers, names, last_price, percent_changes, volume, market_cap = scrape_trending_tickers()
    save_to_sqlite(current_time, tickers, names, last_price, percent_changes, volume, market_cap)
    render_html(current_time, tickers, names, last_price, percent_changes, volume, market_cap)
