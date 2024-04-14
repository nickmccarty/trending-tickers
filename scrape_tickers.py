import requests
from bs4 import BeautifulSoup
from jinja2 import Environment, FileSystemLoader

def scrape_trending_tickers():
    url = "https://finance.yahoo.com/trending-tickers"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    tickers = []
    percent_changes = []

    for row in soup.find_all("tr")[1:]:
        cols = row.find_all("td")
        tickers.append(cols[0].text.strip())
        percent_changes.append(float(cols[1].text.strip("%")))

    return tickers, percent_changes

def render_html(tickers, percent_changes):
    env = Environment(loader=FileSystemLoader("."))
    template = env.get_template("index.html.j2")
    html = template.render(tickers=tickers, percent_changes=percent_changes)

    with open("index.html", "w") as f:
        f.write(html)

if __name__ == "__main__":
    tickers, percent_changes = scrape_trending_tickers()
    render_html(tickers, percent_changes)