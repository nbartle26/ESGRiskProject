from Scraping.spiders.filings_spider import run as run_filings
def fetch_and_store_docs_for_universe():
    # TODO: dynamic universe from company_master; for now run seed
    run_filings()
