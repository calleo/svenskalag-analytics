# Initialize the Pipenv environment
install:
	pipenv lock

# Scrape data from Svenska Lag
scrape:
	pipenv run python scraper/svenska_lag_spider.py

# Run and test all DBT models
dbt_build:
	cd dbt_svenska_lag_analytics && pipenv run dbt build

# Start a DuckDB shell against the DBT output database
query:
	pipenv run duckdb ./dbt_svenska_lag_analytics/target/svenska_lag_analytics.duckdb

csv_export:
	pipenv run duckdb ./dbt_svenska_lag_analytics/target/svenska_lag_analytics.duckdb -s "COPY export_all TO 'data/export_all.csv' (HEADER, DELIMITER ',');"
