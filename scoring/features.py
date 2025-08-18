import pandas as pd, numpy as np, os
from sqlalchemy import create_engine, text

ENGINE = create_engine(os.getenv("DB_URL"))

DIR = {
  ("Carbon Emissions","SCOPE1_INTENSITY"): "LOW_IS_GOOD",
  ("Privacy & Data Security","BREACH_RECORDS"): "LOW_IS_GOOD",
  ("Board","INDEP_DIRECTOR_PCT"): "HIGH_IS_GOOD"
}
ROLE = {
  ("Carbon Emissions","SCOPE1_INTENSITY"): "exposure",
  ("Privacy & Data Security","BREACH_RECORDS"): "exposure",
  ("Board","INDEP_DIRECTOR_PCT"): "management"
}

def build_normalized_features():
    k = pd.read_sql("SELECT * FROM company_kpis", ENGINE)
    if k.empty: return
    # z-score within (issue,kpi_code)
    k["group"] = k["key_issue"] + "::" + k["kpi_code"]
    k["value"] = k["value_norm"].fillna(k["value_raw"])
    k["z"] = k.groupby("group")["value"].transform(lambda s: (s - s.mean())/(s.std(ddof=0)+1e-9))
    k["direction"] = k.apply(lambda r: DIR.get((r.key_issue,r.kpi_code),"LOW_IS_GOOD"), axis=1)
    k["role"] = k.apply(lambda r: ROLE.get((r.key_issue,r.kpi_code),"exposure"), axis=1)

    k["exp_signal"] = np.where(
        (k["role"]=="exposure") & (k["direction"]=="LOW_IS_GOOD"), k["z"],
        np.where(k["role"]=="exposure", -k["z"], 0.0)
    )
    k["mgmt_signal"] = np.where(
        (k["role"]=="management") & (k["direction"]=="HIGH_IS_GOOD"), k["z"],
        np.where(k["role"]=="management", -k["z"], 0.0)
    )

    # Store temp table (optional) or let scoring recompute from SELECT
    with ENGINE.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS kpi_features_temp"))
        con.execute(text("""
            CREATE TABLE kpi_features_temp AS
            SELECT ticker, key_issue,
                   AVG(exp_signal) AS exp_z,
                   AVG(mgmt_signal) AS mgmt_z
            FROM (
              SELECT ticker, key_issue, exp_signal, mgmt_signal FROM (
                SELECT ticker,key_issue,kpi_code,exp_signal,mgmt_signal FROM (
                  SELECT * FROM (SELECT * FROM company_kpis) AS base
                ) AS x
              ) AS y
            ) AS z
            GROUP BY ticker, key_issue
        """))
