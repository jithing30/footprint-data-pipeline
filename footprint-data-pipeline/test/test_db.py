import duckdb

# Connect to your database
con = duckdb.connect("data/gold/footprint.duckdb")

# Query staging view
df_stg = con.execute("SELECT * FROM stg_footprint LIMIT 10").fetchdf()
print(df_stg)

# Query analytics table
df_trends = con.execute("SELECT * FROM country_trends LIMIT 10").fetchdf()
print(df_trends)

con.close()
