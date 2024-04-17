# Trending Tickers

## Motivation

Automated hourly scraping of stock market data and the storage thereof in a database.

## Approach

Using a GitHub Actions workflow, a cron job, and a Python script to automate the scraping of the trending tickers on Yahoo Finance. The data payload is used along with a Jinja template to produce an HTML file that triggers a separate workflow that renders the page using GitHub Pages; the data is inserted into a SQLite database. The Python script also contains logic that uses a library called `feedparser` to iteratively read in the latest news stories by ticker, which can be fed into downstream pipelines.

```mermaid
graph LR;
    GitHub_Actions[GitHub Actions Workflow]-->Cron[Cron Job];
    Cron-->Python_Script[Python Script];
    Python_Script-->Yahoo_Finance[Yahoo Finance];
    Yahoo_Finance-->Data_Payload[Data Payload];
    Data_Payload-->Database[Database];
    Data_Payload-->Jinja_Template[Jinja Template];
    Jinja_Template-->HTML_File[HTML File];
    HTML_File-->GitHub_Pages[GitHub Pages];
    Python_Script-->Feedparser_Library[Feedparser Library];
    Feedparser_Library-->Latest_News[Latest News Stories];
    Latest_News-->Database;
```

## Results

[![](https://img.shields.io/static/v1?label=View%20&message=Chart&labelColor=2f363d&color=blue&style=flat&logo=github&logoColor=959da5)](https://nickmccarty.me/trending-tickers)
<a href="https://colab.research.google.com/drive/1F8uEa79gq1XXPJtnUpsJQ2omooI-oROF?usp=sharing#offline=true&sandboxMode=true" style="text-decoration: none;" target="_blank">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>
