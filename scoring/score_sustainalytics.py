import os, math, numpy as np, pandas as pd
from sqlalchemy import create_engine, text
from datetime import date
ENGINE = create_engine(os.getenv("DB_URL"))

def sigmoid(x, k=0.8): return 1/(1+math.exp(-k*x))

def resolve_materiality_weights():
    # Resolve best-available rules per ticker using fallback logic in Python (simple version)
    cm = pd.read_sql("SELECT ticker,gics_sector,gics_industry_group,gics_industry,gics_sub_industry,peer_group FROM company_master", ENGINE)
    mr = pd.read_sql("SELECT * FROM materiality_rules", ENGINE)
    # Prefer sub_industry → industry → group → sector
    frames = []
    for tier,col in [("sub_industry","gics_sub_industry"),("industry","gics_industry"),
                     ("industry_group","gics_industry_group"),("sector","gics_sector")]:
        j = cm.merge(mr[mr.tier==tier], left_on=col, right_on="tier_value", how="left")
        j["lvl"] = {"sub_industry":1,"industry":2,"industry_group":3,"sector":4}[tier]
        frames.append(j[["ticker","key_issue","pillar","weight","lvl"]])
    cat = pd.concat(frames).dropna(subset=["key_issue"])
    cat["rn"] = cat.groupby(["ticker","key_issue"]).rank("dense", ascending=True, method="first")["lvl"]
    cat = cat[cat["rn"]==1].drop(columns=["rn","lvl"])
    return cat

def compute_and_write_scores():
    issues = pd.read_sql("SELECT * FROM kpi_features_temp", ENGINE)
    if issues.empty: return
    weights = resolve_materiality_weights()
    cm = pd.read_sql("SELECT ticker,peer_group FROM company_master", ENGINE)
    events = pd.read_sql("""
        SELECT ticker, MAX(severity) AS highestControversy
        FROM esg_events
        WHERE event_date >= DATE_SUB(CURDATE(), INTERVAL 24 MONTH)
        GROUP BY ticker
    """, ENGINE)

    # Map to scales
    issues["Exposure_0_10"]       = (5 + 2*issues["exp_z"]).clip(0,10)
    issues["ManagedFraction_0_1"] = issues["mgmt_z"].apply(lambda z: max(0,min(1,sigmoid(z))))
    issues = issues.merge(weights, on=["ticker","key_issue"], how="left")
    # Default even spread if no weights present for ticker
    issues["weight"] = issues.groupby("ticker")["weight"].transform(lambda s: s.fillna(1.0/max(len(s),1)))
    issues["pillar"] = issues["pillar"].fillna("E")
    issues["UnmanagedRisk_k"] = issues["Exposure_0_10"] * (1 - issues["ManagedFraction_0_1"])

    # Per-pillar weighted averages
    def wavg(df, col): return np.average(df[col], weights=df["weight"])
    pillar = issues.groupby(["ticker","pillar"]).apply(lambda df: pd.Series({"pillar_score": wavg(df,"UnmanagedRisk_k")})).reset_index()

    env = pillar[pillar.pillar=="E"][["ticker","pillar_score"]].rename(columns={"pillar_score":"EnvironmentScore"})
    soc = pillar[pillar.pillar=="S"][["ticker","pillar_score"]].rename(columns={"pillar_score":"SocialScore"})
    gov = pillar[pillar.pillar=="G"][["ticker","pillar_score"]].rename(columns={"pillar_score":"GovernanceScore"})
    total = issues.groupby("ticker").apply(lambda df: wavg(df,"UnmanagedRisk_k")).reset_index(name="TotalESG")

    out = cm.merge(env, on="ticker", how="left").merge(soc, on="ticker", how="left").merge(gov, on="ticker", how="left").merge(total,on="ticker",how="left")
    out = out.merge(events, on="ticker", how="left").fillna({"highestControversy":1})
    today = date.today()
    out["ratingYear"] = today.year; out["ratingMonth"] = today.month; out["score_vintage"] = today

    for c in ["TotalESG","EnvironmentScore","SocialScore","GovernanceScore"]:
        out[c] = out[c].round(2)

    issues_out = issues.copy()
    issues_out["score_vintage"] = today

    with ENGINE.begin() as con:
        # issue-level table
        con.execute(text("DELETE FROM issue_scores WHERE score_vintage=:v"), dict(v=today))
        con.execute(text("""
            INSERT INTO issue_scores
            (Ticker,key_issue,pillar,weight,Exposure_0_10,ManagedFraction_0_1,UnmanagedRisk_k,score_vintage)
            VALUES (:t,:k,:p,:w,:e,:m,:u,:v)
        """), issues_out.rename(columns={"ticker":"t","key_issue":"k","pillar":"p","weight":"w",
                                        "Exposure_0_10":"e","ManagedFraction_0_1":"m","UnmanagedRisk_k":"u",
                                        "score_vintage":"v"}).to_dict(orient="records"))

        # company-level table (yfinance shape)
        for _,r in out.iterrows():
            con.execute(text("""
                REPLACE INTO esg_scores_calculated
                (Ticker,TotalESG,EnvironmentScore,SocialScore,GovernanceScore,highestControversy,ratingYear,ratingMonth,peerGroup,score_vintage)
                VALUES (:t,:a,:b,:c,:d,:h,:y,:m,:pg,:v)
            """), dict(t=r.ticker,a=r.TotalESG,b=r.EnvironmentScore,c=r.SocialScore,d=r.GovernanceScore,
                       h=int(r.highestControversy),y=int(r.ratingYear),m=int(r.ratingMonth),pg=r.peer_group or "Unknown",v=today))

