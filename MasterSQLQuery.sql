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

#we want to flag companies involved in our bool columns (animaltesting, coal, alcoholic, etc)
