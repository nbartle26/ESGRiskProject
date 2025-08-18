# phase2AiMlESG.py
from dotenv import load_dotenv
load_dotenv()

import os, urllib.parse as up
import requests
from requests.exceptions import HTTPError, Timeout, RequestException
import hashlib
import datetime as dt
from sqlalchemy import create_engine, text

def get_engine():
    # Prefer DB_URL
    db_url = os.getenv("DB_URL")
    if not db_url:
        host = os.getenv("MYSQL_HOST", "localhost")
        user = os.getenv("MYSQL_USER", "root")
        pwd  = os.getenv("MYSQL_PWD", "")
        db   = os.getenv("MYSQL_DB", "ESGRiskProjectDB")
        pwd_enc = up.quote_plus(pwd or "")
        db_url = f"mysql+mysqlconnector://{user}:{pwd_enc}@{host}:3306/{db}"
    eng = create_engine(db_url)
    # Smoke test
    with eng.connect() as con:
        con.execute(text("SELECT 1"))
    print("DB connection works sucessfully", db_url)
    return eng

ENG = get_engine() 

def save_doc(ticker, url, text, source):
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    with ENG.begin() as con:
        con.execute(text("""
            INSERT INTO raw_documents(ticker,url,fetched_at,mime,sha256,text,year,source)
            VALUES(:t,:u,:f,:m,:s,:x,:y,:src)
            ON DUPLICATE KEY UPDATE text=VALUES(text), fetched_at=VALUES(fetched_at)
        """), dict(
            t=ticker,
            u=url,
            f=dt.datetime.utcnow(),
            m="text/html",
            s=sha,
            x=text,
            y=dt.datetime.utcnow().year,
            src=source
        ))

def fetch_text(url: str) -> str:
    try:
        r = requests.get(
            url,
            timeout=45,
            headers={"User-Agent": "esg-risk-bot/1.0 (+https://example.com/contact)"}
        )
        r.raise_for_status()  # will raise on 4xx/5xx
    except HTTPError as e:
        print(f"[HTTP {r.status_code}] {url} → skipping")
        return ""
    except Timeout:
        print(f"[Timeout] {url} → skipping")
        return ""
    except RequestException as e:
        print(f"[RequestError] {url}: {e} → skipping")
        return ""

    from trafilatura import extract
    txt = extract(r.text, include_formatting=False) or ""
    return txt

def save_if_useful(ticker, url, source):
    text = fetch_text(url)
    if len(text) >= 800:  # sanity filter to avoid garbage/empty pages
        save_doc(ticker, url, text, source)
        print(f"Saved {ticker} - {source} ({len(text)} chars)")
    else:
        print(f"Too short/empty: {ticker} - {source} ({len(text)} chars) → skipped")


if __name__ == "__main__":
    samples = {
    "AAPL": [
        ("https://www.apple.com/environment/", "SustHub"),
        ("https://www.apple.com/environment/pdf/Apple_Environmental_Progress_Report_2025.pdf", "SustReport"),
        ("https://www.sec.gov/ixviewer/ix.html?doc=%2FArchives%2Fedgar%2Fdata%2F0000320193%2F000032019324000006%2Faapl-20231230.htm", "10-K")
    ],
    "MSFT": [
        ("https://www.microsoft.com/en-us/corporate-responsibility/sustainability/report/", "SustReport")
    ]
    }

for ticker, urls in samples.items():
    for url, source in urls:
        save_if_useful(ticker, url, source)

