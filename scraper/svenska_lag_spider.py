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
import hashlib
from dotenv import load_dotenv
from google.cloud import bigquery
from twisted.python.failure import Failure

load_dotenv()

for _loc in ('sv_SE.UTF-8', 'sv_SE.utf8'):  # macOS uses the first form, Linux the second
    try:
        locale.setlocale(locale.LC_TIME, _loc)
        break
    except locale.Error:
        continue
else:
    raise locale.Error("Swedish locale (sv_SE) is not available on this system")
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
    domain = os.getenv("SVENSKALAG_DOMAIN") or "www.ekf.nu"
    team_slug = os.getenv("SVENSKALAG_TEAM_SLUG") or "ekf-fotboll-f2014"
    allowed_domains = [domain]
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
        return f"https://{SvenskaLagSpider.domain}/{SvenskaLagSpider.team_slug}/{path}"

    @staticmethod
    def _is_match(url: str):
        return "/match/" in url

    @staticmethod
    def _is_team_activity(url: str):
        team_path = f"/{SvenskaLagSpider.team_slug}/"
        return team_path in url

    @staticmethod
    def _short_snippet(value: str | None, length: int = 200) -> str:
        if not value:
            return ""
        collapsed = " ".join(value.split())
        return collapsed[:length]

    @staticmethod
    def _html_hash(response: scrapy.http.Response) -> str:
        return hashlib.sha256(response.text.encode("utf-8")).hexdigest()[:12]

    @staticmethod
    def _login_form_count(response: scrapy.http.Response) -> int:
        return len(response.xpath(
            "//form[.//input[@name='UserName'] or .//input[contains(@name,'Pass')] or .//input[@type='password']]"
        ))

    def _log_response_evidence(self, checkpoint: str, response: scrapy.http.Response):
        title = (response.xpath("//title/text()").get() or "").strip()
        canonical = response.xpath("//link[@rel='canonical']/@href").get()
        has_logout = "Logga ut" in response.text
        login_form_count = self._login_form_count(response=response)
        forms = response.xpath("//form")[:5]
        form_evidence = []
        for form in forms:
            input_names = form.xpath(".//input/@name").getall()[:8]
            form_evidence.append(
                {
                    "action": form.xpath("@action").get(),
                    "method": form.xpath("@method").get(),
                    "input_names": input_names
                }
            )
        headers = {
            "content-type": response.headers.get("Content-Type", b"").decode("utf-8", errors="ignore"),
            "server": response.headers.get("Server", b"").decode("utf-8", errors="ignore"),
            "x-cache": response.headers.get("X-Cache", b"").decode("utf-8", errors="ignore"),
            "location": response.headers.get("Location", b"").decode("utf-8", errors="ignore"),
        }
        redirect_urls = response.request.meta.get("redirect_urls", [])
        request_url = response.request.url if response.request else ""
        self.logger.info(
            "[%s] status=%s response_url=%s request_url=%s redirects=%s title=%r canonical=%r "
            "has_logout=%s login_forms=%s html_len=%s html_sha256=%s headers=%s forms=%s",
            checkpoint,
            response.status,
            response.url,
            request_url,
            redirect_urls,
            title,
            canonical,
            has_logout,
            login_form_count,
            len(response.text),
            self._html_hash(response),
            headers,
            form_evidence
        )

    def _assert_authenticated(self, response: scrapy.http.Response, checkpoint: str):
        title = (response.xpath("//title/text()").get() or "").strip()
        canonical = response.xpath("//link[@rel='canonical']/@href").get()
        redirect_urls = response.request.meta.get("redirect_urls", [])
        request_url = response.request.url if response.request else ""
        response_url = response.url
        has_logout = "Logga ut" in response.text
        login_form_count = self._login_form_count(response=response)
        is_login_url = "logga-in" in response_url.lower()
        authenticated = has_logout and not is_login_url
        self.logger.info(
            "[%s] authenticated=%s response_url=%s request_url=%s redirects=%s has_logout=%s login_forms=%s "
            "title=%r canonical=%r",
            checkpoint,
            authenticated,
            response_url,
            request_url,
            redirect_urls,
            has_logout,
            login_form_count,
            title,
            canonical,
        )
        if authenticated:
            return

        body_snippet = self._short_snippet(
            response.xpath("//main").get() or response.xpath("//body").get(),
            length=300
        )
        raise Exception(
            "Login validation failed: "
            f"response_url={response_url} request_url={request_url} redirects={redirect_urls} "
            f"title={title!r} canonical={canonical!r} has_logout={has_logout} "
            f"login_form_count={login_form_count} html_sha256={self._html_hash(response)} "
            f"body_snippet={body_snippet!r}"
        )

    @staticmethod
    def _request_meta() -> dict[str, bool]:
        return {"handle_httpstatus_all": True}

    def _assert_ok_status(self, response: scrapy.http.Response, checkpoint: str):
        if response.status < 400:
            return
        raise Exception(
            f"[{checkpoint}] unexpected_http_status={response.status} url={response.url} "
            f"redirects={response.request.meta.get('redirect_urls', [])} "
            f"body_snippet={self._short_snippet(response.text, length=300)!r}"
        )

    def _request_errback(self, failure: Failure):
        request = failure.request
        response = getattr(failure.value, "response", None)
        status = getattr(response, "status", None)
        response_url = getattr(response, "url", "")
        body_snippet = self._short_snippet(getattr(response, "text", None), length=300) if response else ""
        raise Exception(
            "[REQUEST_ERRBACK] "
            f"request_url={request.url if request else ''} "
            f"response_url={response_url} "
            f"http_status={status} "
            f"error={repr(failure.value)} "
            f"body_snippet={body_snippet!r}"
        ) from failure.value

    @staticmethod
    def _get_current_month_url() -> str:
        current_date = datetime.now()
        year = current_date.strftime('%Y')
        month = current_date.strftime('%B').lower()
        return SvenskaLagSpider._build_url(path=f"kalender/{year}/{month}")

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

    async def start(self) -> Any:
        login_url = self._build_url(path="logga-in")
        username = os.environ["SVENSKALAG_USER"]
        password = os.environ["SVENSKALAG_PASSWORD"]
        self.logger.info(
            "[DEBUG_AUTH_START] login_url=%s domain=%s team_slug=%s start_date=%s",
            login_url,
            self.domain,
            self.team_slug,
            self.start_date.isoformat()
        )
        yield scrapy.FormRequest(
            login_url,
            formdata={'UserName': username, 'UserPass': password},
            callback=self.after_login,
            errback=self._request_errback,
            meta=self._request_meta(),
            dont_filter=True
        )

    def after_login(self, response: scrapy.http.Response, **kwargs: Any):
        self.truncate_files()
        self._assert_ok_status(response=response, checkpoint="AUTH_RESPONSE")
        try:
            login_payload = json.loads(response.text)
        except ValueError as exc:
            raise Exception(
                f"Login response was not JSON: status={response.status} "
                f"content_type={response.headers.get('Content-Type', b'').decode('utf-8', errors='ignore')} "
                f"body_snippet={self._short_snippet(response.text, length=300)!r}"
            ) from exc
        if isinstance(login_payload, dict) and login_payload.get("error"):
            raise Exception(f"Login rejected by server: {login_payload['error']}")
        self.logger.info("[AUTH_RESPONSE] login_ok payload=%s", login_payload)
        probe_url = self._get_current_month_url()
        self.logger.info(
            "[CALENDAR_PROBE] probe_url=%s",
            probe_url
        )
        yield response.follow(
            probe_url,
            callback=self.parse_calendar_probe,
            errback=self._request_errback,
            meta=self._request_meta(),
            dont_filter=True,
        )

    def parse_calendar_probe(self, response: scrapy.http.Response, **kwargs: Any):
        self._log_response_evidence(checkpoint="CALENDAR_PROBE_RESPONSE", response=response)
        self._assert_ok_status(response=response, checkpoint="CALENDAR_PROBE_RESPONSE")
        self._assert_authenticated(response=response, checkpoint="CALENDAR_PROBE_AUTH_CHECK")
        activities_urls = response.xpath(
            './/td/a[@class="invisible-link"]/@href'
        ).getall()
        self.logger.info(
            "[CALENDAR_PROBE_LINKS] calendar_url=%s total_links=%s sample_links=%s",
            response.url,
            len(activities_urls),
            [response.urljoin(url) for url in activities_urls[:10]]
        )
        if not activities_urls:
            self.logger.warning(
                "[CALENDAR_PROBE_NO_ACTIVITY_LINKS] url=%s html_sha256=%s",
                response.url,
                self._html_hash(response),
            )
        start_urls = self._get_start_urls()
        self.logger.info(
            "[AUTH_CHECK] start_urls_count=%s start_urls_sample=%s",
            len(start_urls),
            start_urls[:5]
        )
        yield from response.follow_all(
            start_urls,
            callback=self.parse_calendar,
            errback=self._request_errback,
            meta=self._request_meta(),
        )

    def parse_calendar(self, response: scrapy.http.Response, **kwargs: Any):
        self._log_response_evidence(checkpoint="CALENDAR_RESPONSE", response=response)
        self._assert_ok_status(response=response, checkpoint="CALENDAR_RESPONSE")
        self._assert_authenticated(response=response, checkpoint="AUTH_CHECK")

        activities_urls = response.xpath(
            './/td/a[@class="invisible-link"]/@href'
        ).getall()
        self.logger.info(
            "[CALENDAR_LINKS] calendar_url=%s total_links=%s sample_links=%s",
            response.url,
            len(activities_urls),
            [response.urljoin(url) for url in activities_urls[:10]]
        )
        if not activities_urls:
            self.logger.info(
                "[CALENDAR_NO_ACTIVITY_LINKS] url=%s html_sha256=%s",
                response.url,
                self._html_hash(response),
            )
            return

        # Only include team activities and skip org activities
        team_filter = f"/{self.team_slug}/"
        kept_urls = [url for url in activities_urls if self._is_team_activity(url)]
        removed_urls = [url for url in activities_urls if not self._is_team_activity(url)]
        self.logger.info(
            "[CALENDAR_FILTER] team_slug=%s criteria=%s kept=%s removed=%s kept_sample=%s removed_sample=%s",
            self.team_slug,
            team_filter,
            len(kept_urls),
            len(removed_urls),
            [response.urljoin(url) for url in kept_urls[:5]],
            [response.urljoin(url) for url in removed_urls[:5]]
        )
        activities_urls = kept_urls

        presence_urls = [self._get_presence_url(activity_id=self._get_activity_id(url)) for url
                         in activities_urls]
        self.logger.info(
            "[CALENDAR_PRESENCE] generated=%s sample=%s",
            len(presence_urls),
            presence_urls[:5]
        )
        yield from response.follow_all(
            presence_urls,
            callback=self.parse_presence,
            errback=self._request_errback,
            meta=self._request_meta(),
        )

        activity_urls = []
        for url in activities_urls:
            activity_urls.append(
                self._get_activity_url(activity_id=self._get_activity_id(url),
                                       is_game=self._is_match(url=url))
            )
        self.logger.info(
            "[CALENDAR_ACTIVITY] generated=%s sample=%s",
            len(activity_urls),
            activity_urls[:5]
        )

        yield from response.follow_all(
            activity_urls,
            callback=self.parse_activity,
            errback=self._request_errback,
            meta=self._request_meta(),
        )

    def parse_presence(self, response: scrapy.http.Response, **kwargs: Any):
        self._log_response_evidence(checkpoint="PRESENCE_RESPONSE", response=response)
        self._assert_ok_status(response=response, checkpoint="PRESENCE_RESPONSE")
        init_data = response.xpath('/html/head/script/text()')
        self.logger.info(
            "[PRESENCE_SCRIPTS] url=%s head_script_text_nodes=%s all_script_nodes=%s",
            response.url,
            len(init_data),
            len(response.xpath("//script"))
        )

        for index, data in enumerate(init_data):
            value = data.getall()[0]
            try:
                program = ast_to_dict(calmjs.parse.es5(value))
            except Exception as parse_error:
                self.logger.info(
                    "[PRESENCE_SCRIPT_PARSE_ERROR] url=%s script_index=%s script_len=%s snippet=%r error=%s",
                    response.url,
                    index,
                    len(value),
                    self._short_snippet(value),
                    str(parse_error)
                )
                continue
            if "initData" in program:
                self.logger.info(
                    "[PRESENCE_INITDATA] url=%s initData_keys=%s",
                    response.url,
                    list(program["initData"].keys())[:10] if isinstance(program["initData"], dict) else type(program["initData"])
                )
                self.write(type="presence", object=program["initData"])
                return

        script_evidence = []
        for script in response.xpath("//script")[:8]:
            inline_text = script.xpath("text()").get()
            script_evidence.append(
                {
                    "src": script.xpath("@src").get(),
                    "inline_len": len(inline_text or ""),
                    "inline_snippet": self._short_snippet(inline_text, length=200)
                }
            )
        self.logger.warning(
            "[PRESENCE_INITDATA_MISSING] url=%s scripts=%s",
            response.url,
            script_evidence
        )
        raise Exception(f"Could not find 'initData' on page {response.url}")

    def parse_activity(self, response: scrapy.http.Response, **kwargs: Any):
        self._log_response_evidence(checkpoint="ACTIVITY_RESPONSE", response=response)
        self._assert_ok_status(response=response, checkpoint="ACTIVITY_RESPONSE")
        ng_data_init = response.xpath("//body/@data-ng-init")
        ng_data_init_value = ng_data_init.get()
        if not ng_data_init_value:
            body = response.xpath("//body")
            body_attrs = body[0].attrib if body else {}
            candidate_attrs = {k: self._short_snippet(v, length=120) for k, v in body_attrs.items()
                               if "init" in k.lower() or "ng" in k.lower() or "data" in k.lower()}
            self.logger.warning(
                "[ACTIVITY_INIT_MISSING] url=%s body_attrs=%s candidate_attrs=%s body_snippet=%r",
                response.url,
                list(body_attrs.keys()),
                candidate_attrs,
                self._short_snippet(response.xpath("//body").get(), length=300)
            )
            raise Exception(f"Could not find body/@data-ng-init on page {response.url}")

        try:
            js = ast_to_dict(calmjs.parse.es5(ng_data_init_value))
        except Exception as parse_error:
            self.logger.warning(
                "[ACTIVITY_INIT_PARSE_ERROR] url=%s init_len=%s init_snippet=%r error=%s",
                response.url,
                len(ng_data_init_value),
                self._short_snippet(ng_data_init_value),
                str(parse_error)
            )
            raise
        self.logger.info(
            "[ACTIVITY_INIT_FOUND] url=%s keys=%s",
            response.url,
            list(js.keys())[:10]
        )
        for key, value in js.items():
            activity_data = value[0][1][0]
            self.write(type="activity", object=activity_data)

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
    process = CrawlerProcess(
        settings={
            "LOG_LEVEL": os.getenv("SCRAPY_LOG_LEVEL", "INFO"),
            "COOKIES_DEBUG": os.getenv("SCRAPY_COOKIES_DEBUG", "0") == "1",
            "LOGSTATS_INTERVAL": float(os.getenv("SCRAPY_LOGSTATS_INTERVAL", "60.0")),
        }
    )
    process.crawl(SvenskaLagSpider)
    process.start()

    activity_count = len(DATA["activity"])
    presence_count = len(DATA["presence"])
    print(f"Scrape debug payload counts: activities={activity_count}, presences={presence_count}")
    if activity_count == 0 or presence_count == 0:
        print(f"Scrape debug payload sample: activity={DATA['activity'][:1]}, presence={DATA['presence'][:1]}")
        raise Exception(f"Scrape payload is empty: activities={activity_count}, presences={presence_count}")

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
    print(f"Create raw table for activities, with {activity_count} rows")
    print(f"Create raw table for presences, with {presence_count} rows")

    def load_bq_table(bq_client: bigquery.Client, file_path: str, table_id: str):
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
