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


class StringifyShirtNumbersTests(unittest.TestCase):
    def test_converts_nested_integer_shirt_numbers_to_strings(self):
        presence = {
            "teams": [
                {
                    "attendingMembers": [
                        {"memberId": 1, "shirtNumber": 7},
                        {"memberId": 2, "shirtNumber": "10"},
                        {"memberId": 3, "shirtNumber": None},
                    ]
                }
            ]
        }

        result = svenska_lag_spider.SvenskaLagSpider._stringify_shirt_numbers(presence)

        attending_members = result["teams"][0]["attendingMembers"]
        self.assertEqual(attending_members[0]["shirtNumber"], "7")
        self.assertEqual(attending_members[1]["shirtNumber"], "10")
        self.assertIsNone(attending_members[2]["shirtNumber"])

    def test_leaves_objects_without_shirt_number_unchanged(self):
        presence = {"scheduleId": 123, "teamId": 456}

        result = svenska_lag_spider.SvenskaLagSpider._stringify_shirt_numbers(presence)

        self.assertEqual(result, {"scheduleId": 123, "teamId": 456})


if __name__ == "__main__":
    unittest.main()
