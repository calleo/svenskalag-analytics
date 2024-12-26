import calmjs.parse
import scrapy
from typing import Any
import os
from scrapy.crawler import CrawlerProcess
import json
from calmjs.parse.unparsers.extractor import ast_to_dict
import pathlib
from datetime import datetime, timezone
import locale
import duckdb
import uuid
from google.cloud import bigquery

locale.setlocale(locale.LC_TIME, 'sv_SE') # Get month names in Swedish
DATA = {
    "activity": [],
    "presence": []
}
DUCKDB_FILE_PATH = 'data/svenska_lag.duckdb'
ACTIVITY_FILE_PATH = 'temp/activity.json'
PRESENCE_FILE_PATH = 'temp/presence.json'
LOAD_ID = str(uuid.uuid4())
LOAD_TIMESTAMP = datetime.now(timezone.utc).isoformat()

class SvenskaLagSpider(scrapy.Spider):
    name = "SvenskaLag"
    allowed_domains = [os.getenv("SVENSKALAG_DOMAIN")]
    team_slug = os.getenv("SVENSKALAG_TEAM_SLUG")
    start_date = datetime.fromisoformat(os.getenv("SVENSKALAG_START_DATE"))

    @staticmethod
    def _get_activity_id(url: str) -> str:
        return url.split("/")[-2]

    @staticmethod
    def _get_presence_url(activity_id: str) -> str:
        return SvenskaLagSpider._build_url(path=f"controlpanel/presence/chooseparticipants/{activity_id}")

    @staticmethod
    def _get_activity_url(activity_id: str, is_game: bool) -> str:
        if is_game:
            return SvenskaLagSpider._build_url(path=f"match/redigera/{activity_id}")
        else:
            return SvenskaLagSpider._build_url(path=f"kalender/redigera-aktivitet/{activity_id}")

    @staticmethod
    def _build_url(path: str) -> str:
        return f"https://{SvenskaLagSpider.allowed_domains[0]}/{SvenskaLagSpider.team_slug}/{path}"

    @staticmethod
    def _is_match(url: str):
        return "/match/" in url

    @staticmethod
    def _is_team_activity(url: str):
        return "/ekf-fotboll-f2014/" in url

    @staticmethod
    def _get_start_urls() -> list[str]:
        end_date = datetime.now()
        urls = []
        current_date = SvenskaLagSpider.start_date
        while current_date <= end_date:
            year = current_date.strftime('%Y')
            month = current_date.strftime('%B').lower()
            urls.append(SvenskaLagSpider._build_url(path=f"kalender/{year}/{month}"))
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        return urls

    def start_requests(self) -> Any:
        login_url = "https://www.ekf.nu/ekf-fotboll-f2014/logga-in"
        username = os.getenv("SVENSKALAG_USER")
        password = os.getenv("SVENSKALAG_PASSWORD")
        yield scrapy.FormRequest(
            login_url,
            formdata={'UserName': username, 'UserPass': password},
            callback=self.parse
        )

    def parse(self, response: scrapy.http.Response, **kwargs: Any):
        self.truncate_files()
        yield from response.follow_all(SvenskaLagSpider._get_start_urls(), callback=self.parse_calendar)

    @staticmethod
    def parse_calendar(response: scrapy.http.Response, **kwargs: Any):
        if not "Logga ut" in response.text:
            raise Exception("Inloggning misslyckades")

        activities_urls = response.xpath(
            './/td/a[@class="invisible-link"]/@href'
        ).getall()

        # Only include team activities and skip org activities
        activities_urls = [url for url in activities_urls if SvenskaLagSpider._is_team_activity(url)]

        presence_urls = [SvenskaLagSpider._get_presence_url(activity_id=SvenskaLagSpider._get_activity_id(url)) for url
                         in activities_urls]
        yield from response.follow_all(presence_urls, callback=SvenskaLagSpider.parse_presence)

        activity_urls = []
        for url in activities_urls:
            activity_urls.append(
                SvenskaLagSpider._get_activity_url(activity_id=SvenskaLagSpider._get_activity_id(url),
                                                   is_game=SvenskaLagSpider._is_match(url=url))
            )

        yield from response.follow_all(activity_urls, callback=SvenskaLagSpider.parse_activity)

    @staticmethod
    def parse_presence(response: scrapy.http.Response, **kwargs: Any):
        init_data = response.xpath('/html/head/script/text()')

        for data in init_data:
            value = data.getall()[0]
            program = ast_to_dict(calmjs.parse.es5(value))
            if "initData" in program:
                SvenskaLagSpider.write(type="presence", object=program["initData"])
                return

        raise Exception(f"Could not find 'initData' on page {response.url}")

    @staticmethod
    def parse_activity(response: scrapy.http.Response, **kwargs: Any):
        ng_data_init = response.xpath("//body/@data-ng-init")
        js = ast_to_dict(calmjs.parse.es5(ng_data_init.getall()[0]))
        for key, value in js.items():
            activity_data = value[0][1][0]
            SvenskaLagSpider.write(type="activity", object=activity_data)

    @staticmethod
    def write(type: str, object: dict):
        object["load_id"] = LOAD_ID
        object["load_timestamp"] = LOAD_TIMESTAMP
        DATA[type].append(object)

    @staticmethod
    def truncate_files():
        for data_file in [ACTIVITY_FILE_PATH, PRESENCE_FILE_PATH]:
            data_file = pathlib.Path(data_file)
            if data_file.exists():
                data_file.unlink()


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(SvenskaLagSpider)
    process.start()

    with open(ACTIVITY_FILE_PATH, "w") as f:
        for activity in DATA["activity"]:
            f.write(json.dumps(activity) + "\n")

    with open(PRESENCE_FILE_PATH, "w") as f:
        for presence in DATA["presence"]:
            f.write(json.dumps(presence) + "\n")

    # Load data into DuckDB
    conn = duckdb.connect(DUCKDB_FILE_PATH)
    conn.execute(f"CREATE OR REPLACE TABLE raw_activity AS SELECT * FROM read_json_auto('{ACTIVITY_FILE_PATH}')")
    conn.execute(f"CREATE OR REPLACE TABLE raw_presence AS SELECT * FROM read_json_auto('{PRESENCE_FILE_PATH}')")

    activity_count = conn.execute("SELECT COUNT(*) FROM raw_activity").fetchone()[0]
    presence_count = conn.execute("SELECT COUNT(*) FROM raw_presence").fetchone()[0]
    print(f"Create raw table for activities, with a {activity_count} rows")
    print(f"Create raw table for presences, with a {presence_count} rows")

    def load_bq_table(bq_client: bigquery.Client, file_path: str, table_id: str):
        # Truncate
        #query = bq_client.query(f"DROP TABLE IF EXISTS {table_id}")
        #query.result()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition="WRITE_APPEND",
            autodetect=True,
            schema_update_options=["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_ADDITION"]
        )
        with open(file_path, "rb") as source_file:
            job = bq_client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )
            result = job.result()
            print(f"Load Job Result: {result} [{result.errors or ''}]")

        query = bq_client.query(f"SELECT COUNT(*) AS ROW_COUNT FROM {table_id}")
        result = next(query.result())

        print(f"Loaded data from {file_path} into {table_id}. Row count: {result['ROW_COUNT']}")

    # Load data into BigQuery
    dataset_id = "ekf-f2014.dwh"
    activity_table_id = f"{dataset_id}.raw_activity"
    presence_table_id = f"{dataset_id}.raw_presence"

    # Create the dataset if it doesn't exist
    client = bigquery.Client()
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"  # Adjust location as needed
    client.create_dataset(dataset, exists_ok=True)

    # Load the preprocessed file into BigQuery
    load_bq_table(bq_client=client, file_path=ACTIVITY_FILE_PATH, table_id=activity_table_id)
    load_bq_table(bq_client=client, file_path=PRESENCE_FILE_PATH, table_id=presence_table_id)
