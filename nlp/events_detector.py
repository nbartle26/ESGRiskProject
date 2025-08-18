from sqlalchemy import create_engine, text
import re, datetime as dt, os

ENGINE = create_engine(os.getenv("DB_URL"))

def severity_from_text(s: str) -> int:
    s = s.lower()
    if "recall" in s or "class action" in s or "data breach" in s: return 4
    if "fine" in s or "penalty" in s: return 3
    return 2

def extract_and_upsert_events():
    with ENGINE.begin() as con:
        rows = con.execute(text("SELECT doc_id,ticker,text FROM raw_documents")).fetchall()
        for doc_id,ticker,txt in rows:
            for m in re.finditer(r"(recall|data breach|regulatory fine|penalty|lawsuit)[^.]{0,200}\.", txt, flags=re.I):
                ev_txt = m.group(0)
                sev = severity_from_text(ev_txt)
                con.execute(text("""
                    INSERT INTO esg_events(ticker,event_date,key_issue,severity,amount_usd,source_doc_id)
                    VALUES(:t,:d,:k,:s,:a,:doc)
                """), dict(t=ticker,d=dt.date.today(),k="Privacy & Data Security" if "breach" in ev_txt.lower() else "Business Ethics",
                           s=sev,a=None,doc=doc_id))
