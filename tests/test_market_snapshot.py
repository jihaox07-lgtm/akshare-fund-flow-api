import importlib.util
import asyncio
import time
import unittest
from unittest.mock import patch


def load_normalizer():
    spec = importlib.util.find_spec("market_snapshot")
    if spec is None:
        raise AssertionError("缺少全市场行情标准化模块")
    from market_snapshot import normalize_market_records

    return normalize_market_records


def load_provider_runner():
    spec = importlib.util.find_spec("market_snapshot")
    if spec is None:
        raise AssertionError("缺少全市场行情标准化模块")
    try:
        from market_snapshot import fetch_market_records
    except ImportError as exc:
        raise AssertionError("缺少行情源自动降级函数") from exc

    return fetch_market_records


class MarketSnapshotNormalizationTests(unittest.TestCase):
    def test_normalizes_eastmoney_columns_and_rejects_invalid_rows(self) -> None:
        normalize_market_records = load_normalizer()
        rows = normalize_market_records([
            {
                "代码": "600000",
                "名称": "浦发银行",
                "最新价": 10.2,
                "涨跌幅": 2.1,
                "成交额": 1_200_000_000,
                "换手率": 1.5,
                "量比": 1.2,
            },
            {"代码": "", "名称": "无效样本", "最新价": 1, "涨跌幅": 1, "成交额": 1},
            {"代码": "000001", "名称": "平安银行", "最新价": "-", "涨跌幅": 1, "成交额": 1},
        ])

        self.assertEqual(rows, [{
            "code": "600000",
            "name": "浦发银行",
            "price": 10.2,
            "change_pct": 2.1,
            "turnover": 1_200_000_000.0,
            "turnover_rate": 1.5,
            "volume_ratio": 1.2,
        }])

    def test_accepts_sina_style_columns_as_provider_fallback(self) -> None:
        normalize_market_records = load_normalizer()
        rows = normalize_market_records([{
            "code": "000001",
            "name": "平安银行",
            "trade": "12.30",
            "changepercent": "-1.2",
            "amount": "800000000",
            "turnoverratio": "2.2",
        }])

        self.assertEqual(rows[0]["code"], "000001")
        self.assertEqual(rows[0]["price"], 12.3)
        self.assertEqual(rows[0]["change_pct"], -1.2)
        self.assertIsNone(rows[0]["volume_ratio"])

    def test_provider_failure_falls_back_without_returning_partial_invalid_data(self) -> None:
        fetch_market_records = load_provider_runner()

        def failed_provider():
            raise RuntimeError("主源不可用")

        source, rows = fetch_market_records([
            ("AKShare / 东方财富", failed_provider),
            ("AKShare / 新浪财经", lambda: [{
                "code": "600000",
                "name": "浦发银行",
                "trade": "10.2",
                "changepercent": "2.1",
                "amount": "1200000000",
                "turnoverratio": "1.5",
            }]),
        ], minimum_rows=1)

        self.assertEqual(source, "AKShare / 新浪财经")
        self.assertEqual(rows[0]["code"], "600000")


class TencentMetricNormalizationTests(unittest.TestCase):
    def test_parses_turnover_volume_ratio_and_source_time(self) -> None:
        from tencent_metrics import parse_tencent_metrics

        fields = [""] * 50
        fields[1] = "示例股票"
        fields[2] = "600000"
        fields[30] = "20260923151720"
        fields[38] = "3.25"
        fields[49] = "1.68"
        raw = f'v_sh600000="{"~".join(fields)}";'

        self.assertEqual(parse_tencent_metrics(raw), [{
            "code": "600000",
            "turnover_rate": 3.25,
            "volume_ratio": 1.68,
            "source_updated_at": "2026-09-23T15:17:20+08:00",
        }])

    def test_sanitizes_codes_and_limits_batch_size(self) -> None:
        from tencent_metrics import sanitize_codes

        codes = ["600000", "300750", "600000", "bad", "123"]
        self.assertEqual(sanitize_codes(codes), ["600000", "300750"])
        self.assertEqual(len(sanitize_codes([f"{index:06d}" for index in range(150)])), 120)


class MarketSnapshotCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        import main

        main.market_cache = None
        if main.market_refresh_task and not main.market_refresh_task.done():
            main.market_refresh_task.cancel()
        main.market_refresh_task = None

    async def test_concurrent_cache_misses_share_one_upstream_request(self) -> None:
        import main

        calls = 0

        def slow_loader():
            nonlocal calls
            calls += 1
            time.sleep(0.05)
            return "AKShare / 新浪财经", [{"code": "600000"}]

        with patch.object(main, "load_market_snapshot", slow_loader):
            first, second = await asyncio.gather(
                main.cached_market_snapshot(),
                main.cached_market_snapshot(),
            )

        self.assertEqual(calls, 1)
        self.assertEqual(first, second)

    async def test_expired_cache_returns_immediately_and_refreshes_in_background(self) -> None:
        import main

        old_rows = [{"code": "600000"}]
        new_rows = [{"code": "000001"}]
        main.market_cache = (
            time.time() - main.MARKET_CACHE_SECONDS - 1,
            "AKShare / 旧缓存",
            old_rows,
        )

        def slow_loader():
            time.sleep(0.05)
            return "AKShare / 新行情", new_rows

        with patch.object(main, "load_market_snapshot", slow_loader):
            started = time.perf_counter()
            result = await main.cached_market_snapshot()
            elapsed = time.perf_counter() - started
            self.assertEqual(result[1:], ("AKShare / 旧缓存", old_rows))
            self.assertEqual(result[0], main.market_cache[0])
            self.assertLess(elapsed, 0.02)
            self.assertIsNotNone(main.market_refresh_task)
            await asyncio.wait_for(main.market_refresh_task, timeout=1)

        self.assertEqual(main.market_cache[1:], ("AKShare / 新行情", new_rows))

    async def test_refresh_timeout_keeps_single_flight_until_worker_finishes(self) -> None:
        import main

        calls = 0

        def slow_loader():
            nonlocal calls
            calls += 1
            time.sleep(0.05)
            return "AKShare / 新浪财经", [{"code": "600000"}]

        with (
            patch.object(main, "load_market_snapshot", slow_loader),
            patch.object(main, "MARKET_REFRESH_TIMEOUT_SECONDS", 0.005),
        ):
            with self.assertRaises(Exception):
                await main.cached_market_snapshot()
            first_task = main.market_refresh_task
            with self.assertRaises(Exception):
                await main.cached_market_snapshot()
            self.assertIs(main.market_refresh_task, first_task)
            await asyncio.wait_for(first_task, timeout=1)

        self.assertEqual(calls, 1)

    async def test_failed_background_refresh_preserves_stale_cache(self) -> None:
        import main

        old_rows = [{"code": "600000"}]
        cached_at = time.time() - main.MARKET_CACHE_SECONDS - 1
        main.market_cache = (cached_at, "AKShare / 旧缓存", old_rows)

        with patch.object(main, "load_market_snapshot", side_effect=RuntimeError("上游失败")):
            result = await main.cached_market_snapshot()
            await asyncio.wait_for(main.market_refresh_task, timeout=1)

        self.assertEqual(result, (cached_at, "AKShare / 旧缓存", old_rows))
        self.assertEqual(main.market_cache, (cached_at, "AKShare / 旧缓存", old_rows))

    async def test_endpoint_reports_cache_generation_time_not_request_time(self) -> None:
        import main

        cached_at = time.time() - 3600
        main.market_cache = (cached_at, "AKShare / 旧缓存", [{"code": "600000"}])
        with patch.object(main, "MARKET_CACHE_SECONDS", 7200):
            payload = await main.market_snapshot()
        self.assertEqual(
            payload["updated_at"],
            main.datetime.fromtimestamp(cached_at, main.timezone.utc).isoformat(),
        )


if __name__ == "__main__":
    unittest.main()
