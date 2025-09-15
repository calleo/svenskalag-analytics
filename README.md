# Svenskalag Analytics

Ever wanted to extract data from Svenskalag to build custom reports and perform analysis which is not supported out of the box.

This repository contains code to extract data and transform it into analytics ready tables.

This project has nothing to do with the company behind [Svenskalag.se](https://www.svenskalag.se).

🍐

## Introduction

This is the result of a weekend project. As you might expect from such projects, corners have been cut to quickly solve problems and reach the wanted results. The motivational driver behind the project was to analyze training and game attendance, as well as learning a bit about web scraping and DuckDB.

## Architecture

### Scraper

Built using Python and Scrapy. It crawls your team page and extracts information about activities, members and presence data. Data is persisted into a DuckDB database. This is quite brittle, and could stop working at any time (hopefully it's caught by the error handling in Python or the tests created in DBT).

### DBT Project

DBT is used to transform the raw data into analytics ready tables. Analytical data models are stored as DuckDB databases (locally).

## Getting Started

You need to have the following software installed. It has only been tested on MacOS:

* Python. Install using [Pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation).
* [Pipenv](https://github.com/pypa/pipenv)
* [DuckDB](https://duckdb.org/docs/installation)
* [Git](https://docs.github.com/en/get-started/getting-started-with-git/set-up-git)
* [Make](https://www.gnu.org/software/make/) Instead of installing Make, you may also look at the individual commands in the [Makefile](./Makefile), to see how the steps are performed.

### Step 1 - Clone

Clone this repository, by running `git clone https://github.com/calleo/svenskalag-analytics.git` from the terminal. Ensure the correct [Python version](./.python-version) is installed, then run `pipenv lock` to install the needed packages.

### Step 2 - Scrape

Set proper values for the environment variables by copying `.env.tamplate` and naming the copy `.env`.

* **SVENSKALAG_USER**: Your Svenskalag username
* **SVENSKALAG_PASSWORD**: Your password to Svenskalag
* **SVENSKALAG_START_DATE**: The date from which you would like to fetch data, for example "2021-01-01"
* **SVENSKA_LAG_DOMAIN**: The domain where your Svenskalag site is hosted, for example "www.difinnebandy.se"
* **SVENSKALAG_TEAM_SLUG**: The part of the domain which points to your team, for example "difibs-herr" (full URL is https://www.difinnebandy.se/difibs-herr)

Start the scraper by running `make scrape` from within the newly cloned directory.

### Step 3 - DBT

Run `make dbt_build` runs DBT and produces the tables used for the analysis.

### Step 3 - Analyze

Running `make query` will start the DuckDB client against the database which DBT created. Run a query, for example `SELECT * FROM member LIMIT 11`.

If you prefer to import the data into a spreadsheet application (say Microsoft Excel), run `make csv_export` to get a CSV file (stored in `./data` folder) which you can then import into the application.
