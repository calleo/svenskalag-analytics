# Initialize the Pipenv environment
install:
	pipenv install

# Scrape data from Svenska Lag
scrape:
	pipenv run python scraper/svenska_lag_spider.py

# Run and test all DBT models
dbt_build:
	cd dbt_svenska_lag_analytics && pipenv run dbt build

dbt_debug_bq:
	cd dbt_svenska_lag_analytics_bq && pipenv run dbt --debug debug

# Run and test all DBT models
dbt_build_bq:
	cd dbt_svenska_lag_analytics_bq && pipenv run dbt build

# Start a DuckDB shell against the DBT output database
query:
	pipenv run duckdb ./dbt_svenska_lag_analytics/target/svenska_lag_analytics.duckdb

csv_export:
	pipenv run duckdb ./dbt_svenska_lag_analytics/target/svenska_lag_analytics.duckdb -s "COPY export_all TO 'data/export_all.csv' (HEADER, DELIMITER ',');"

rm_docker:
	docker rm svenska-lag-ekf-metabase --force

build_metabase: rm_docker
	docker buildx build --file ./metabase/Dockerfile --tag svenska-lag-ekf-metabase .

start_metabase: rm_docker
	docker run -d -p 3000:3000 -v ./metabase/data:/metabase-data --name svenska-lag-ekf-metabase svenska-lag-ekf-metabase
