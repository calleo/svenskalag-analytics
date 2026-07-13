import importlib
import os
import unittest
from unittest.mock import patch

from scrapy.http import HtmlResponse, Request


os.environ.setdefault("SVENSKALAG_START_DATE", "2021-01-01")

with patch("locale.setlocale", return_value="sv_SE.utf8"):
    svenska_lag_spider = importlib.import_module("scraper.svenska_lag_spider")


class CalendarEmptyMonthTests(unittest.TestCase):
    def _response(self, url: str) -> HtmlResponse:
        request = Request(url=url, meta={"redirect_urls": []})
        body = "<html><body>Logga ut</body></html>"
        return HtmlResponse(url=url, body=body.encode("utf-8"), encoding="utf-8", request=request)

    def test_parse_calendar_does_not_fail_when_month_has_no_activity_links(self):
        spider = svenska_lag_spider.SvenskaLagSpider()
        response = self._response("https://example.com/calendar")

        output = list(spider.parse_calendar(response))

        self.assertEqual(output, [])

    def test_parse_calendar_probe_does_not_fail_when_current_month_has_no_activity_links(self):
        spider = svenska_lag_spider.SvenskaLagSpider()
        response = self._response("https://example.com/probe")

        with patch.object(spider, "_get_start_urls", return_value=[]):
            output = list(spider.parse_calendar_probe(response))

        self.assertEqual(output, [])


if __name__ == "__main__":
    unittest.main()
