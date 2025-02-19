import sqlite3
import pandas as pd

# Create database file
conn = sqlite3.connect("dem_database.db")  
cursor = conn.cursor()

# Import CSVs as tables
for i, file in enumerate(["DEM_COUNTRY.csv", "DEM_LABEL.csv", "DEM_DATA_NATIONAL.csv"]):
    table_name = file.replace(".csv", "")  # Remove ".csv" from file name
    df = pd.read_csv(file)
    df.to_sql(table_name, conn, if_exists="replace", index=False)


# 1. join dem_country to dem_data_national to get the full names of countries
# 2. remove non-arctic countries from this joined table
# 3. join dem_labels to resulting arctic table from step 2
# 4. see which indicators all of these countries have in common?

# step 1
cursor.execute("""
CREATE TEMP TABLE full_country_names as
SELECT dcn.COUNTRY_NAME_EN, ddn.*
FROM DEM_DATA_NATIONAL as ddn
JOIN DEM_COUNTRY as dcn
ON ddn.COUNTRY_ID = dcn.COUNTRY_ID;
""")

# step 2, delete from step 1
arctic_countries = (
    "Faroe Islands", "Siberia", "Finland",
    "International", "Norway", "Canada", "Sweden",
    "Iceland", "United States", "Russia"
)

cursor.execute(f"""
DELETE FROM full_country_names
WHERE COUNTRY_NAME_EN NOT IN {str(arctic_countries)};
""")

# step 3
cursor.execute("""
CREATE TEMP TABLE arctic_labelled AS
SELECT fcn.*, dl.INDICATOR_LABEL_EN
FROM full_country_names fcn
JOIN DEM_LABEL dl
ON fcn.INDICATOR_ID = dl.INDICATOR_ID;
""")

query = "SELECT * FROM arctic_labelled" # return the result table

df = pd.read_sql_query(query, conn)
df.to_csv("arctic_indicators.csv", index=False) # Save results as a CSV file

query_2023 = """SELECT COUNTRY_NAME_EN, INDICATOR_LABEL_EN, COUNT(*)
FROM arctic_labelled
WHERE YEAR = 2023
GROUP BY COUNTRY_NAME_EN, INDICATOR_LABEL_EN
HAVING COUNT(*) = 1;"""

df = pd.read_sql_query(query_2023, conn)
df.to_csv("arctic_2023_indicators.csv", index=False) # Save results as a CSV file

conn.close()