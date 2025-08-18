import os, sys, time
from dotenv import load_dotenv
load_dotenv()

from scraping.parse_docs import fetch_and_store_docs_for_universe
from nlp.classify_issues import tag_issue_snippets
from nlp.extract_kpis import extract_and_upsert_kpis
from nlp.events_detector import extract_and_upsert_events
from scoring.features import build_normalized_features
from scoring.score_sustainalytics import compute_and_write_scores

def main():
    # 1) Fetch docs (10-K, sustainability, news) and store text in raw_documents
    fetch_and_store_docs_for_universe()

    # 2) GenAI/NLP: classify text into the 33 issues, store snippets (in-memory or a table if you prefer)
    tag_issue_snippets()

    # 3) GenAI+regex: extract numeric KPIs to company_kpis; detect controversies to esg_events
    extract_and_upsert_kpis()
    extract_and_upsert_events()

    # 4) Build normalized features (z-scores, directionality) for exposure/management
    build_normalized_features()

    # 5) Compute Exposure_k, ManagedFraction_k, UnmanagedRisk_k; aggregate → pillar + TotalESG; upsert tables
    compute_and_write_scores()

    print("Pipeline complete sanity check")
if __name__ == "__main__":
    main()
