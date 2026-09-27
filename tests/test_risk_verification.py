import asyncio
from datetime import date
import unittest
from unittest.mock import AsyncMock, patch

import requests


AS_OF = date(2026, 9, 24)
CALENDAR = [date(2026, 9, 24), date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30), date(2026, 10, 8), date(2026, 10, 9), date(2026, 10, 12)]


class FakeSource:
    def __init__(self):
        from risk_verification import Coverage

        self.calendar = CALENDAR
        self.profiles = {
            "600000": {"code": "600000", "name": "浦发银行", "listed_on": "1999-11-10", "quote_date": "2026-09-24", "volume": 100, "total_shares": 10_000_000, "source_updated_at": "2026-09-24T15:00:00+08:00"},
            "000001": {"code": "000001", "name": "平安银行", "listed_on": "1991-04-03", "quote_date": "2026-09-24", "volume": 100, "total_shares": 10_000_000, "source_updated_at": "2026-09-24T15:00:00+08:00"},
        }
        self.suspension_rows = Coverage([], True)
        self.delisted_rows = Coverage([], True)
        self.announcement_rows = Coverage([], True)
        self.unlock_rows = Coverage([], True)

    def load_calendar(self):
        return self.calendar

    def load_profile(self, code):
        return self.profiles[code]

    def load_suspensions(self, as_of):
        return self.suspension_rows

    def load_delisted(self, code):
        return self.delisted_rows

    def load_announcements(self, code, start, end):
        return self.announcement_rows

    def load_unlocks(self, code):
        return self.unlock_rows


def checks(report):
    return {item["id"]: item for item in report["checks"]}


class RiskVerificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_negative_lists_do_not_clear_unbounded_key_event_risk(self):
        from risk_verification import RiskVerifier

        report = (await RiskVerifier(FakeSource(), today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]

        self.assertEqual(report["overall"], "pending")
        self.assertEqual(checks(report)["key_event"]["state"], "unknown")
        self.assertEqual(checks(report)["unlock"]["state"], "pass")
        self.assertEqual(report["unlockRatio"], 0.0)
        self.assertEqual(report["windowEnd"], "2026-10-09")
        self.assertEqual(len(report["checks"]), 5)
        for item in report["checks"]:
            self.assertTrue(item["source"])
            self.assertTrue(item["retrievedAt"])
            self.assertTrue(item["reason"])

    async def test_st_and_delisting_are_each_hard_risks(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.profiles["600000"]["name"] = "*ST浦银"
        source.delisted_rows = Coverage([{"code": "000001", "delisted_on": "2026-09-24"}], True)
        reports = await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000", "000001"], AS_OF)

        self.assertEqual(checks(reports[0])["special_treatment"]["state"], "risk")
        self.assertEqual(checks(reports[1])["listing"]["state"], "risk")
        self.assertIsNone(checks(reports[1])["listing"]["sourceUpdatedAt"])
        self.assertIn("szse.cn", checks(reports[1])["listing"]["sourceUrl"])
        self.assertEqual([r["overall"] for r in reports], ["rejected", "rejected"])

    async def test_active_suspension_is_risk_but_no_volume_alone_is_unknown(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.suspension_rows = Coverage([{"code": "600000", "start": "2026-09-24", "end": None, "reason": "刊登重要公告"}], True)
        source.profiles["000001"]["volume"] = 0
        reports = await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000", "000001"], AS_OF)

        self.assertEqual(checks(reports[0])["trading"]["state"], "risk")
        self.assertEqual(checks(reports[1])["trading"]["state"], "unknown")

    async def test_predicted_resume_date_does_not_prove_a_suspension_ended(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.suspension_rows = Coverage([{"code": "600000", "start": "2026-09-24", "end": None,
                                            "resume": "2026-09-24", "reason": "重大事项"}], True)
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        self.assertEqual(checks(report)["trading"]["state"], "risk")

    async def test_risk_announcement_rejects_even_if_pagination_incomplete(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.announcement_rows = Coverage([{"code": "600000", "date": "2026-09-18", "title": "浦发银行关于被立案调查的公告", "url": "https://example.test/notice"}], False, "第二页不可用")
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]

        self.assertEqual(checks(report)["key_event"]["state"], "risk")
        self.assertIsNone(checks(report)["key_event"]["sourceUpdatedAt"])
        self.assertEqual(report["overall"], "rejected")

    async def test_new_independent_investigation_is_not_hidden_by_old_risk_resolution(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.announcement_rows = Coverage([{"code": "600000", "date": "2026-09-18",
                                              "title": "关于撤销旧风险警示及被立案调查的公告"}], True)
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        self.assertEqual(checks(report)["key_event"]["state"], "risk")

    async def test_multiple_unlock_events_use_same_total_shares_denominator(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.unlock_rows = Coverage([
            {"code": "600000", "date": "2026-09-28", "shares": 1_000_000},
            {"code": "600000", "date": "2026-10-09", "shares": 900_000},
        ], True)
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]

        self.assertEqual(checks(report)["unlock"]["state"], "risk")
        self.assertEqual(report["unlockRatio"], 19.0)
        self.assertEqual(report["unlockRatioBasis"], "total_shares")
        self.assertEqual(report["overall"], "rejected")

    async def test_unlock_threshold_compares_unrounded_percent(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.unlock_rows = Coverage([{"code": "600000", "date": "2026-09-28", "shares": 1_799_999.999}], True)
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        self.assertEqual(checks(report)["unlock"]["state"], "pass")
        self.assertLess(report["unlockRatio"], 18)

    async def test_incomplete_queue_and_missing_denominator_never_pass(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.unlock_rows = Coverage([], False, "分页不完整")
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        self.assertEqual(checks(report)["unlock"]["state"], "unknown")

    async def test_known_large_unlock_rejects_even_with_incomplete_pages_without_claiming_exact_ratio(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.unlock_rows = Coverage([{"code": "600000", "date": "2026-09-28", "shares": 2_000_000}], False, "末页超时")
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        self.assertEqual(checks(report)["unlock"]["state"], "risk")
        self.assertEqual(report["overall"], "rejected")
        self.assertIsNone(report["unlockRatio"])
        self.assertIn("至少", checks(report)["unlock"]["reason"])
        self.assertIsNone(report["unlockRatio"])

        source = FakeSource()
        source.profiles["600000"]["total_shares"] = None
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        self.assertEqual(checks(report)["unlock"]["state"], "unknown")

    async def test_calendar_failure_does_not_guess_five_trading_days(self):
        from risk_verification import RiskVerifier

        source = FakeSource()
        source.calendar = []
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        self.assertIsNone(report["windowEnd"])
        self.assertEqual(checks(report)["unlock"]["state"], "unknown")

    async def test_one_profile_failure_preserves_other_stock(self):
        from risk_verification import RiskVerifier

        source = FakeSource()
        del source.profiles["600000"]
        reports = await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000", "000001"], AS_OF)
        self.assertEqual([r["code"] for r in reports], ["600000", "000001"])
        self.assertEqual(checks(reports[0])["listing"]["state"], "unknown")
        self.assertIn("KeyError", checks(reports[0])["listing"]["reason"])
        self.assertEqual(checks(reports[1])["listing"]["state"], "pass")

    async def test_stale_quote_cannot_clear_listing_trading_or_st(self):
        from risk_verification import RiskVerifier

        source = FakeSource()
        source.profiles["600000"]["quote_date"] = "2026-09-23"
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        states = checks(report)
        for check_id in ("listing", "trading", "special_treatment", "unlock"):
            self.assertEqual(states[check_id]["state"], "unknown")

    async def test_incomplete_delisting_list_and_zero_volume_do_not_clear_listing(self):
        from risk_verification import Coverage, RiskVerifier

        source = FakeSource()
        source.delisted_rows = Coverage([], False, "退市名单分页未核实")
        source.profiles["600000"]["volume"] = 0
        report = (await RiskVerifier(source, today=date(2026, 9, 26)).verify(["600000"], AS_OF))[0]
        self.assertEqual(checks(report)["listing"]["state"], "unknown")

    async def test_current_trading_day_is_recomputed_on_each_request(self):
        from risk_verification import RiskVerifier

        source = FakeSource()
        clock = [date(2026, 9, 24)]
        verifier = RiskVerifier(source, today_provider=lambda: clock[0])
        first = (await verifier.verify(["600000"], AS_OF))[0]
        self.assertEqual(checks(first)["unlock"]["state"], "pass")
        clock[0] = date(2026, 9, 28)
        source.profiles["600000"]["quote_date"] = "2026-09-28"
        second = (await verifier.verify(["600000"], date(2026, 9, 28)))[0]
        self.assertEqual(checks(second)["unlock"]["state"], "pass")

    async def test_holiday_as_of_cannot_be_treated_as_latest_trading_day(self):
        from risk_verification import RiskVerifier

        report = (await RiskVerifier(FakeSource(), today=date(2026, 9, 26)).verify(["600000"], date(2026, 9, 25)))[0]
        self.assertEqual(checks(report)["unlock"]["state"], "unknown")

    async def test_single_flight_shares_batch_wide_list_requests(self):
        from risk_verification import RiskVerifier

        source = FakeSource()
        calls = 0

        def slow_suspensions(as_of):
            nonlocal calls
            calls += 1
            import time
            time.sleep(0.04)
            return source.suspension_rows

        source.load_suspensions = slow_suspensions
        verifier = RiskVerifier(source, today=date(2026, 9, 26))
        await asyncio.gather(verifier.verify(["600000"], AS_OF), verifier.verify(["000001"], AS_OF))
        self.assertEqual(calls, 1)

    async def test_queued_work_is_not_started_after_request_timeout(self):
        from risk_verification import RiskVerifier
        import time

        verifier = RiskVerifier(FakeSource(), today=date(2026, 9, 26))
        verifier._semaphore = asyncio.Semaphore(1)
        verifier.source_timeout_seconds = 0.02
        called = []

        first = asyncio.create_task(verifier._load(("slow",), lambda: time.sleep(0.08)))
        await asyncio.sleep(0.005)
        second = asyncio.create_task(verifier._load(("queued",), lambda: called.append("late")))
        await asyncio.gather(first, second, return_exceptions=True)
        await asyncio.sleep(0.09)
        self.assertEqual(called, [])

    async def test_failed_upstream_is_briefly_cached_to_prevent_retry_storm(self):
        from risk_verification import FetchFailure, RiskVerifier

        verifier = RiskVerifier(FakeSource(), today=date(2026, 9, 26))
        calls = 0

        def failed():
            nonlocal calls
            calls += 1
            raise requests.HTTPError("503")

        first = await verifier._load(("failed",), failed)
        second = await verifier._load(("failed",), failed)
        self.assertIsInstance(first, FetchFailure)
        self.assertIsInstance(second, FetchFailure)
        self.assertEqual(calls, 1)


class RiskSourceBoundaryTests(unittest.TestCase):
    def test_invalid_batch_input_is_rejected_without_silent_dropping(self):
        from risk_verification import parse_codes

        for value in ("", "600000,bad", "600000,600000", ",600000", "600000;000001", ",".join(f"{i:06d}" for i in range(31))):
            with self.subTest(value=value[:40]), self.assertRaises(ValueError):
                parse_codes(value)
        self.assertEqual(parse_codes("600000,000001"), ["600000", "000001"])

    def test_incomplete_upstream_pagination_is_not_complete_evidence(self):
        from risk_verification import EastmoneyRiskSource

        source = EastmoneyRiskSource()
        source._get_json = lambda url, params: {"success": True, "result": {"count": 2, "pages": 2, "data": [{"SECURITY_CODE": "600000", "FREE_DATE": "2026-09-28", "ABLE_FREE_SHARES": 10}]}}
        result = source.load_unlocks("600000")
        self.assertFalse(result.complete)

    def test_explicit_complete_empty_page_is_distinct_from_null_result(self):
        from risk_verification import EastmoneyRiskSource

        source = EastmoneyRiskSource()
        source._get_json = lambda url, params: {"success": True, "result": {"count": 0, "pages": 0, "data": []}}
        self.assertTrue(source.load_unlocks("600000").complete)
        source._get_json = lambda url, params: {"success": False, "result": None}
        self.assertFalse(source.load_unlocks("600000").complete)

    def test_503_and_wrong_unlock_fields_do_not_produce_complete_evidence(self):
        from risk_verification import EastmoneyRiskSource

        source = EastmoneyRiskSource()

        def failed(url, params):
            raise requests.HTTPError("503")

        source._get_json = failed
        self.assertFalse(source.load_unlocks("600000").complete)
        source._get_json = lambda url, params: {"success": True, "result": {"count": 1, "pages": 1, "data": [{"SECURITY_CODE": "600000", "FREE_DATE": "bad", "ABLE_FREE_SHARES": 100}]}}
        self.assertFalse(source.load_unlocks("600000").complete)

    def test_eastmoney_requests_have_a_shared_minimum_interval(self):
        from risk_verification import EastmoneyRiskSource
        import time

        source = EastmoneyRiskSource()
        source.request_gap_seconds = 0.03
        seen = []

        class Response:
            def raise_for_status(self):
                pass

            def json(self):
                return {"success": True}

        def fake_get(*args, **kwargs):
            seen.append(time.perf_counter())
            return Response()

        with patch.object(source.session, "get", fake_get):
            source._get_json("https://example.test/data", {})
            source._get_json("https://example.test/data", {})
        self.assertGreaterEqual(seen[1] - seen[0], 0.025)


class RiskRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_rejects_invalid_code_and_date_before_any_upstream_call(self):
        from fastapi import HTTPException
        import main

        with patch.object(main, "risk_verifier") as verifier:
            with self.assertRaises(HTTPException) as bad_codes:
                await main.stock_risk("600000,bad", "2026-09-25")
            with self.assertRaises(HTTPException) as bad_date:
                await main.stock_risk("600000", "2026-09-31")
            with self.assertRaises(HTTPException) as future_date:
                await main.stock_risk("600000", "2099-01-01")
            with self.assertRaises(HTTPException) as ancient_date:
                await main.stock_risk("600000", "0001-01-01")
            verifier.verify.assert_not_called()
        self.assertEqual(bad_codes.exception.status_code, 400)
        self.assertEqual(bad_date.exception.status_code, 422)
        self.assertEqual(future_date.exception.status_code, 422)
        self.assertEqual(ancient_date.exception.status_code, 422)

    async def test_route_degrades_batch_failure_to_five_unknown_checks_per_code(self):
        import main

        with patch.object(main, "risk_verifier") as verifier:
            verifier.verify.side_effect = RuntimeError("unexpected batch failure")
            payload = await main.stock_risk("600000,000001", "2026-09-24")

        self.assertEqual(payload["count"], 2)
        self.assertEqual([row["code"] for row in payload["data"]], ["600000", "000001"])
        for row in payload["data"]:
            self.assertEqual(row["overall"], "pending")
            self.assertEqual(len(row["checks"]), 5)
            self.assertTrue(all(item["state"] == "unknown" for item in row["checks"]))
            self.assertTrue(all("批量核验异常" in item["reason"] for item in row["checks"]))

    async def test_route_recursively_sanitizes_non_finite_values(self):
        import json
        import main

        report = {
            "code": "600000",
            "overall": "pending",
            "checks": [
                {
                    "key": "unlock",
                    "state": "unknown",
                    "reason": "上游返回异常数值",
                    "evidence": {"unlockRatio": float("nan"), "history": [float("inf")]},
                }
            ],
        }
        with patch.object(main.risk_verifier, "verify", new_callable=AsyncMock) as verify:
            verify.return_value = [report]
            payload = await main.stock_risk("600000", "2026-09-24")

        self.assertIsNone(payload["data"][0]["checks"][0]["evidence"]["unlockRatio"])
        self.assertIsNone(payload["data"][0]["checks"][0]["evidence"]["history"][0])
        json.dumps(payload, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
