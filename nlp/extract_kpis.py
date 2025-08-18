import re, statistics
from sqlalchemy import create_engine, text
import os

ENGINE = create_engine(os.getenv("DB_URL"))

# Minimal KPI map; expand via config/kpi_map.yaml
KPI_PATTERNS = {
  ("Carbon Emissions","SCOPE1_INTENSITY"): r"(scope\s*1).{0,40}?(\d[\d,\.]*)(?:\s*(t|tonnes|metric tons))",
  ("Privacy & Data Security","BREACH_RECORDS"): r"(breach(ed)?\s+.*?(\d[\d,\.]*))\s+(records|customers|users)"
}

def call_llm_kpi_extractor(text_chunk:str, key_issue:str)->list[tuple[str,float,str]]:
    # Return list of (kpi_code, value, units)
    return []

def extract_and_upsert_kpis():
    with ENGINE.begin() as con:
        docs = con.execute(text("SELECT doc_id,ticker,text,year FROM raw_documents")).fetchall()
        for doc_id,ticker,txt,year in docs:
            for (issue,kpi), pattern in KPI_PATTERNS.items():
                for m in re.finditer(pattern, txt, flags=re.I|re.S):
                    # Naive parse
                    val = float(m.group(2).replace(",",""))
                    con.execute(text("""
                        INSERT INTO company_kpis(ticker,fiscal_year,key_issue,kpi_code,value_raw,source_doc_id,confidence)
                        VALUES(:t,:y,:i,:k,:v,:d,0.60)
                        ON DUPLICATE KEY UPDATE value_raw=VALUES(value_raw), source_doc_id=VALUES(source_doc_id)
                    """), dict(t=ticker,y=year,i=issue,k=kpi,v=val,d=doc_id))
            # LLM fallback per issue:
            # for issue in ...:
            #   for kpi_code, val, units in call_llm_kpi_extractor(txt, issue): upsert...
