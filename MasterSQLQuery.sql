Use ESGRiskProjectDB;
SELECT Count(*) AS Total_Records From ESG_Scores;
SELECT * FROM ESG_Scores LIMIT 20;

#lets group companies by industry type and get a count
SELECT PeerGroup, Count(*) AS CompanyCount
FROM ESG_Scores
GROUP BY PeerGroup
ORDER BY CompanyCount DESC;


#lets group companies by their average scores in different columns
SELECT PeerGroup,
	AVG(TotalESG) AS AvgTotalESG,
	AVG(EnvironmentScore) AS AvgEnvironmentScore,
	AVG(SocialScore) AS AvgSocialScore,
	AVG(GovernanceScore) AS AvgGovernanceScore
FROM ESG_Scores
GROUP BY PeerGroup
ORDER BY AvgTotalESG DESC;


#now lets generate a list of companies with high controversy and some boolean columns to provide context
SELECT Ticker, PeerGroup, TotalESG, HighestControversy, RelatedControversy, Alcoholic, AnimalTesting, Coal, Tobacco
FROM ESG_Scores
WHERE HighestControversy >= 3
ORDER BY HighestControversy DESC, TotalESG ASC
LIMIT 20;

ALTER TABLE esg_scores
  ADD COLUMN gics_sector           VARCHAR(64),
  ADD COLUMN gics_industry_group   VARCHAR(64),
  ADD COLUMN gics_industry         VARCHAR(64),
  ADD COLUMN gics_sub_industry     VARCHAR(64);
CREATE INDEX ix_company_gics 
ON company_master(gics_sub_industry, gics_industry, gics_industry_group, gics_sector);

CREATE TABLE IF NOT EXISTS materiality_rules (
  tier ENUM('sub_industry','industry','industry_group','sector') NOT NULL,
  tier_value VARCHAR(128) NOT NULL,
  key_issue VARCHAR(64) NOT NULL,     
  pillar  ENUM('E','S','G') NOT NULL,
  weight  DECIMAL(6,5) NOT NULL,            -- weights per rule-set should sum to 1
  UNIQUE KEY (tier, tier_value, key_issue)
);

ALTER TABLE raw_documents
  ADD UNIQUE KEY ux_raw_ticker_sha (ticker, sha256);

