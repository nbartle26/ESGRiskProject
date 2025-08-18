# phase2AiMlESG.py

from dotenv import load_dotenv
load_dotenv()

import os, urllib.parse as up
import requests
import hashlib
import datetime as dt
from sqlalchemy import create_engine, text
from trafilatura import extract

# --- DB connection setup ---
def get_engine():
    db_url = os.getenv("DB_URL")
    if not db_url:
        host = os.getenv("MYSQL_HOST", "localhost")
        user = os.getenv("MYSQL_USER", "root")
        pwd  = os.getenv("MYSQL_PWD", "")
        db   = os.getenv("MYSQL_DB", "ESGRiskProjectDB")
        pwd_enc = up.quote_plus(pwd or "")
        db_url = f"mysql+mysqlconnector://{user}:{pwd_enc}@{host}:3306/{db}"
    eng = create_engine(db_url)
    with eng.connect() as con:
        con.execute(text("SELECT 1"))
    print("DB connection works sucessfully", db_url)
    return eng

ENG = get_engine()

# --- save a document into raw_documents ---
def save_doc(ticker, url, bodytext, source):
    sha = hashlib.sha256(bodytext.encode("utf-8")).hexdigest()
    with ENG.begin() as con:
        con.execute(text("""
            INSERT INTO raw_documents(ticker,url,fetched_at,mime,sha256,text,year,source)
            VALUES(:t,:u,:f,:m,:s,:x,:y,:src)
            ON DUPLICATE KEY UPDATE text=VALUES(text), fetched_at=VALUES(fetched_at)
        """),
        dict(
            t=ticker,
            u=url,
            f=dt.datetime,
            m="text/html",
            s=sha,
            x=bodytext,
            y=dt.datetime.year,
            src=source
        ))

# --- fetch page content (HTML) ---
def fetch_bodytext(url: str) -> str:
    r = requests.get(url, timeout=45, headers={"User-Agent":"esg-risk-bot"})
    r.raise_for_status()
    return extract(r.text, include_formatting=False) or ""

# --- wrapper that skips short/empty docs ---
def save_if_useful(ticker, url, source):
    bodytext = fetch_bodytext(url)
    if len(bodytext) >= 700:
        save_doc(ticker, url, bodytext, source)
        print(f"Saved {ticker} - {source} ({len(bodytext)} chars)")
    else:
        print(f"Too short/empty: {ticker} - {source} ({len(bodytext)} chars) → skipped")

# --- sample test run ---
samples = {
    "AAPL": [
        ("https://www.apple.com/environment/", "SustHub")
    ],
    "MSFT": [
        ("https://www.microsoft.com/en-us/corporate-responsibility/sustainability/report/", "SustReport")
    ]
}

if __name__ == "__main__":
    for ticker, urls in samples.items():
        for url, source in urls:
            save_if_useful(ticker, url, source)
