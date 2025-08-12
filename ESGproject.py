import os
import math
import numpy as np
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

#resources

#guide to esg variables
#https://finance.yahoo.com/news/guide-understanding-esg-ratings-151501443.html?guccounter=1&guce_referrer=aHR0cHM6Ly93d3cuZ29vZ2xlLmNvbS8&guce_referrer_sig=AQAAAIIpgB-tCzQci4M377vJNG3vWi-8XmUd5DDqrF_bRJpxLJwQKcsc8BUFpQbsirDDo9J-XA7oGK9DRShtmHPEEvBasmmPkPi8uT365-UitsrdQQnQtz2NZyGoGF7-Gpyvc_EtSzpm_BJcZbz9v7xV867guT_9Xd7mT0oS0fZytzrE


load_dotenv() 
DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PWD  = os.getenv("MYSQL_PWD")
DB_NAME = os.getenv("MYSQL_DB", "ESGRiskProjectDB")


#create dataframe
esg_df = pd.read_csv(r"C:\Users\Owner\Desktop\PostGrad\DataProjects\ESGRiskProject\sp500_esg_scores.csv")

#print(list(esg_df.columns))
esg_df = esg_df.drop_duplicates()
esg_df = esg_df.rename(columns={
    'totalEsg': 'TotalESG',
    'environmentScore': 'EnvironmentScore',
    'socialScore': 'SocialScore',
    'governanceScore': 'GovernanceScore',
    'highestControversy': 'HighestControversy',
    'relatedControversy': 'RelatedControversy',
    'ratingYear': 'RatingYear',
    'ratingMonth': 'RatingMonth',
    'peerGroup': 'PeerGroup',
    'esgPerformance': 'ESGPerformance',
    'peerCount': 'PeerCount',
    'environmentPercentile': 'EnvironmentPercentile',
    'socialPercentile': 'SocialPercentile',
    'governancePercentile': 'GovernancePercentile',
    'alcoholic': 'Alcoholic',
    'animalTesting': 'AnimalTesting',
    'adult': 'Adult',
    'controversialWeapons': 'ControversialWeapons',
    'furLeather': 'FurAndLeather',
    'smallArms': 'SmallArms',
    'gambling': 'Gambling',
    'coal': 'Coal',
    'gmo': 'GMO',
    'militaryContract': 'OnMilitaryContract',
    'nuclear': 'Nuclear',
    'tobacco': 'Tobacco'
})

# normalize booleans -> 0/1 (keeps None for missing)
bool_cols = ['Alcoholic','AnimalTesting','Adult','ControversialWeapons','FurAndLeather','SmallArms',
             'Gambling','Coal','GMO','OnMilitaryContract','Nuclear','Tobacco']
for c in bool_cols:
    esg_df[c] = esg_df[c].map({True:1, False:0, 'TRUE':1, 'True':1, 'FALSE':0, 'False':0}).astype('float').where(esg_df[c].notna(), np.nan)


cols = [
    "Ticker","TotalESG","EnvironmentScore","SocialScore","GovernanceScore",
    "HighestControversy","RelatedControversy","RatingYear","RatingMonth",
    "PeerGroup","ESGPerformance","PeerCount",
    "EnvironmentPercentile","SocialPercentile","GovernancePercentile",
    "Alcoholic","AnimalTesting","Adult","ControversialWeapons","FurAndLeather",
    "SmallArms","Gambling","Coal","GMO","OnMilitaryContract","Nuclear","Tobacco"
]

# Prepare each value, converting NaN and None into SQL NULL so SQL doesn't reject them
def fix_nan(v):
    if v is None: return None
    try:
        return None if (isinstance(v, float) and math.isnan(v)) else v
    except:
        return v
    


placeholders = ", ".join(["%s"]*len(cols))
sql = f"""
INSERT INTO ESG_Scores ({", ".join(cols)})
VALUES ({placeholders})
ON DUPLICATE KEY UPDATE
  TotalESG=VALUES(TotalESG),
  EnvironmentScore=VALUES(EnvironmentScore),
  SocialScore=VALUES(SocialScore),
  GovernanceScore=VALUES(GovernanceScore),
  HighestControversy=VALUES(HighestControversy),
  RelatedControversy=VALUES(RelatedControversy),
  PeerGroup=VALUES(PeerGroup),
  ESGPerformance=VALUES(ESGPerformance),
  PeerCount=VALUES(PeerCount),
  EnvironmentPercentile=VALUES(EnvironmentPercentile),
  SocialPercentile=VALUES(SocialPercentile),
  GovernancePercentile=VALUES(GovernancePercentile),
  Alcoholic=VALUES(Alcoholic),
  AnimalTesting=VALUES(AnimalTesting),
  Adult=VALUES(Adult),
  ControversialWeapons=VALUES(ControversialWeapons),
  FurAndLeather=VALUES(FurAndLeather),
  SmallArms=VALUES(SmallArms),
  Gambling=VALUES(Gambling),
  Coal=VALUES(Coal),
  GMO=VALUES(GMO),
  OnMilitaryContract=VALUES(OnMilitaryContract),
  Nuclear=VALUES(Nuclear),
  Tobacco=VALUES(Tobacco);
"""

# --- DB bootstrap + batch upsert ---
root_conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PWD)
root_cur = root_conn.cursor()
root_cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
root_cur.close(); root_conn.close()

conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PWD, database=DB_NAME)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS ESG_Scores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Ticker VARCHAR(10),
    TotalESG FLOAT,
    EnvironmentScore FLOAT,
    SocialScore FLOAT,
    GovernanceScore FLOAT,
    HighestControversy INT,
    RelatedControversy TEXT,
    RatingYear INT,
    RatingMonth INT,
    PeerGroup VARCHAR(255),
    ESGPerformance VARCHAR(50),
    PeerCount INT,
    EnvironmentPercentile FLOAT,
    SocialPercentile FLOAT,
    GovernancePercentile FLOAT,
    Alcoholic BOOLEAN,
    AnimalTesting BOOLEAN,
    Adult BOOLEAN,
    ControversialWeapons BOOLEAN,
    FurAndLeather BOOLEAN,
    SmallArms BOOLEAN,
    Gambling BOOLEAN,
    Coal BOOLEAN,
    GMO BOOLEAN,
    OnMilitaryContract BOOLEAN,
    Nuclear BOOLEAN,
    Tobacco BOOLEAN,
    UNIQUE KEY uq_ticker_period (Ticker, RatingYear, RatingMonth)
)
""")

rows = [tuple(fix_nan(row.get(c)) for c in cols) for _, row in esg_df.iterrows()]
cur.executemany(sql, rows)
conn.commit()
print(f"Upserted {len(rows)} rows.")

cur.close(); #release the cursor
conn.close(); #release the connection


# #Build and connect to MySQL database
# conn = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="CrimsonTide2025#",
# )
# #Think of cursor as a "Database Command Center", 
# # .cursor() is a method that creates a cursor object to interact with the database
# cursor = conn.cursor()
# #sanity test
# print("Connected to MySQL Server")
# # cursor.execute() executes a db command
# # Create database if it doesn't exist
# cursor.execute("CREATE DATABASE IF NOT EXISTS ESGRiskProjectDB")
# print("Database 'ESGRiskProjectDB' created or already exists")
# cursor.close()
# conn.close()

# conn = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="CrimsonTide2025#",
#     database="ESGRiskProjectDB"
# )
# cursor = conn.cursor()

# # Create table with expanded schema
# cursor.execute("""
# CREATE TABLE IF NOT EXISTS ESG_Scores (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     Ticker VARCHAR(10),
#     TotalESG FLOAT,
#     EnvironmentScore FLOAT,
#     SocialScore FLOAT,
#     GovernanceScore FLOAT,
#     HighestControversy INT,
#     RelatedControversy Text,
#     RatingYear INT,
#     RatingMonth INT,
#     PeerGroup VARCHAR(255),
#     ESGPerformance VARCHAR(50),
#     PeerCount INT,
#     EnvironmentPercentile FLOAT,
#     SocialPercentile FLOAT,
#     GovernancePercentile FLOAT,
#     Alcoholic BOOLEAN,
#     AnimalTesting BOOLEAN,
#     Adult BOOLEAN,
#     ControversialWeapons BOOLEAN,
#     FurAndLeather BOOLEAN,
#     SmallArms Boolean,
#     Gambling BOOLEAN,
#     Coal BOOLEAN,
#     GMO BOOLEAN,
#     OnMilitaryContract BOOLEAN,
#     Nuclear BOOLEAN,
#     Tobacco BOOLEAN
# )
# """)
# print("Table 'ESG_Scores' created or already exists")


# #read in the columns, the data for each column, and insert into the table
# #looping through each row the dataframe 
# # where "_" is our index, and row is the actual data from the datafram
# for _, row in esg_df.iterrows():
#     sql = """
#     INSERT INTO ESG_Scores (
#         Ticker, TotalESG, EnvironmentScore, SocialScore, GovernanceScore, HighestControversy, RelatedControversy,
#         RatingYear, RatingMonth, PeerGroup, ESGPerformance, PeerCount,
#         EnvironmentPercentile, SocialPercentile, GovernancePercentile,
#         Alcoholic, AnimalTesting, Adult, ControversialWeapons, FurAndLeather, SmallArms, Gambling, Coal, GMO, OnMilitaryContract, Nuclear, Tobacco
#     )  VALUES (""" + ", ".join(["%s"]*27) + """)
# ON DUPLICATE KEY UPDATE
#   TotalESG=VALUES(TotalESG),
#   EnvironmentScore=VALUES(EnvironmentScore),
#   SocialScore=VALUES(SocialScore),
#   GovernanceScore=VALUES(GovernanceScore),
#   HighestControversy=VALUES(HighestControversy),
#   RelatedControversy=VALUES(RelatedControversy),
#   PeerGroup=VALUES(PeerGroup),
#   ESGPerformance=VALUES(ESGPerformance),
#   PeerCount=VALUES(PeerCount),
#   EnvironmentPercentile=VALUES(EnvironmentPercentile),
#   SocialPercentile=VALUES(SocialPercentile),
#   GovernancePercentile=VALUES(GovernancePercentile),
#   Alcoholic=VALUES(Alcoholic),
#   AnimalTesting=VALUES(AnimalTesting),
#   Adult=VALUES(Adult),
#   ControversialWeapons=VALUES(ControversialWeapons),
#   FurAndLeather=VALUES(FurAndLeather),
#   SmallArms=VALUES(SmallArms),
#   Gambling=VALUES(Gambling),
#   Coal=VALUES(Coal),
#   GMO=VALUES(GMO),
#   OnMilitaryContract=VALUES(OnMilitaryContract),
#   Nuclear=VALUES(Nuclear),
#   Tobacco=VALUES(Tobacco);
# """

#     values = tuple(fix_nan(row.get(col)) for col in [
#         'Ticker',
#         'TotalESG',
#         'EnvironmentScore',
#         'SocialScore',
#         'GovernanceScore',
#         'HighestControversy',
#         'RelatedControversy',
#         'RatingYear',
#         'RatingMonth',
#         'PeerGroup',
#         'ESGPerformance',
#         'PeerCount',
#         'EnvironmentPercentile',
#         'SocialPercentile',
#         'GovernancePercentile',
#         'Alcoholic',
#         'AnimalTesting',
#         'Adult',
#         'ControversialWeapons',
#         'FurAndLeather',
#         'SmallArms',
#         'Gambling',
#         'Coal',
#         'GMO',
#         'OnMilitaryContract',
#         'Nuclear',
#         'Tobacco'
#     ])
# #insert the values into the table
#     cursor.execute(sql, values)

# conn.commit()
# print(f"Inserted {cursor.rowcount} records into ESG_Scores")

# cursor.close()
# conn.close()
# print("ESG data successfully loaded into MySQL!")
