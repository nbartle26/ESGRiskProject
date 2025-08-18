import requests, hashlib, datetime as dt
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import os

ENGINE = create_engine(os.getenv("DB_URL"))

def save_doc(ticker, url, mime, text_content, year, source):
    sha = hashlib.sha256(text_content.encode("utf-8")).hexdigest()
    with ENGINE.begin() as con:
        con.execute(text("""
            INSERT INTO raw_documents(ticker,url,fetched_at,mime,sha256,text,year,source)
            VALUES(:t,:u,:f,:m,:s,:x,:y,:src)
            ON DUPLICATE KEY UPDATE text=VALUES(text), fetched_at=VALUES(fetched_at)
        """), dict(t=ticker,u=url,f=dt.datetime.utcnow(),m=mime,s=sha,x=text_content,y=year,src=source))

def get_text(url):
    r = requests.get(url, timeout=30); r.raise_for_status()
    from trafilatura import extract
    txt = extract(r.text, include_formatting=False) or ""
    return txt

def crawl_company_docs(ticker, urls):
    for u, source in urls:
        try:
            txt = get_text(u)
            if len(txt) > 500:
                year = dt.datetime.utcnow().year
                save_doc(ticker, u, "text/html", txt, year, source)
        except Exception as e:
            print("Fetch error", ticker, u, e)

# Example seed
SEED = {
  "AAPL": [("https://www.apple.com/investor/10k-2024.html","10-K"),
           ("https://www.apple.com/sustainability/","SustReport")]
}

def run():
    for t, urls in SEED.items():
        crawl_company_docs(t, urls)
