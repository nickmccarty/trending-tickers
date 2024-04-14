import sqlite3
import requests
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader

def scrape_trending_tickers():
    url = "https://finance.yahoo.com/trending-tickers"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    tickers = []
    percent_changes = []

    def scrape_trending_tickers():
    url = "https://finance.yahoo.com/trending-tickers"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    tickers = []
    percent_changes = []

    for row in soup.find_all("tr")[1:]:
      cols = row.find_all("td")
      ticker = cols[0].text.strip()
      percent_change_str = cols[1].text.strip("%")

      try:
          percent_change = float(percent_change_str)
      except ValueError:
          # If conversion fails, skip this row
          continue

      tickers.append(ticker)
      percent_changes.append(percent_change)

    return tickers, percent_changes

def save_to_sqlite(tickers, percent_changes):
    db_file = 'tickers.db'
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # Create the table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS trending_tickers
                 (ticker TEXT, percent_change REAL)''')

    # Insert the data
    for ticker, percent_change in zip(tickers, percent_changes):
        c.execute("INSERT INTO trending_tickers (ticker, percent_change) VALUES (?, ?)", (ticker, percent_change))

    conn.commit()
    conn.close()

def render_html(tickers, percent_changes):
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("index.html.j2")
    html = template.render(tickers=tickers, percent_changes=percent_changes)

    with open("index.html", "w") as f:
        f.write(html)

if __name__ == "__main__":
    tickers, percent_changes = scrape_trending_tickers()
    save_to_sqlite(tickers, percent_changes)
    render_html(tickers, percent_changes)
