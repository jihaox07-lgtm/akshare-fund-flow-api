import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import math
import time
from typing import Any, Callable

import akshare as ak
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from market_snapshot import fetch_market_records
from tencent_metrics import fetch_tencent_metrics, sanitize_codes


@asynccontextmanager
async def lifespan(_: FastAPI):
    warmup = asyncio.create_task(warm_market_cache())
    yield
    if not warmup.done():
        warmup.cancel()
    await asyncio.gather(warmup, return_exceptions=True)
    if market_refresh_task and not market_refresh_task.done():
        market_refresh_task.cancel()
        await asyncio.gather(market_refresh_task, return_exceptions=True)


app = FastAPI(title="A股板块资金流 API", version="1.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

CACHE_SECONDS = 60
MARKET_CACHE_SECONDS = 600
MARKET_REFRESH_TIMEOUT_SECONDS = 90
cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
market_cache: tuple[float, str, list[dict[str, Any]]] | None = None
market_refresh_task: asyncio.Task[tuple[float, str, list[dict[str, Any]]]] | None = None


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        return clean_value(value.item())
    return value


def dataframe_records(fetcher: Callable[..., Any], period: str) -> list[dict[str, Any]]:
    frame = fetcher(symbol=period)
    return [
        {str(key): clean_value(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


async def cached_flow(kind: str, period: str) -> list[dict[str, Any]]:
    key = f"{kind}:{period}"
    cached = cache.get(key)
    if cached and time.time() - cached[0] < CACHE_SECONDS:
        return cached[1]

    fetcher = ak.stock_fund_flow_industry if kind == "industry" else ak.stock_fund_flow_concept
    try:
        rows = await asyncio.wait_for(
            asyncio.to_thread(dataframe_records, fetcher, period),
            timeout=25,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"上游公开数据暂不可用：{type(exc).__name__}",
        ) from exc

    cache[key] = (time.time(), rows)
    return rows


def market_dataframe_records(fetcher: Callable[[], Any]) -> list[dict[str, Any]]:
    frame = fetcher()
    return frame.to_dict(orient="records")


def load_market_snapshot() -> tuple[str, list[dict[str, Any]]]:
    providers: list[tuple[str, Callable[[], list[dict[str, Any]]]]] = []
    for source, function_name in (
        ("AKShare / 新浪财经公开行情", "stock_zh_a_spot"),
        ("AKShare / 东方财富公开行情", "stock_zh_a_spot_em"),
    ):
        fetcher = getattr(ak, function_name, None)
        if callable(fetcher):
            providers.append((source, lambda fetcher=fetcher: market_dataframe_records(fetcher)))
    return fetch_market_records(providers)


async def refresh_market_snapshot() -> tuple[float, str, list[dict[str, Any]]]:
    global market_cache
    try:
        source, rows = await asyncio.to_thread(load_market_snapshot)
    except Exception as exc:
        if market_cache:
            return market_cache
        raise HTTPException(
            status_code=502,
            detail=f"全市场公开行情暂不可用：{type(exc).__name__}",
        ) from exc
    market_cache = (time.time(), source, rows)
    return market_cache


def start_market_refresh() -> asyncio.Task[tuple[float, str, list[dict[str, Any]]]]:
    global market_refresh_task
    if market_refresh_task is None or market_refresh_task.done():
        market_refresh_task = asyncio.create_task(refresh_market_snapshot())

        def consume_exception(task: asyncio.Task[Any]) -> None:
            if not task.cancelled():
                task.exception()

        market_refresh_task.add_done_callback(consume_exception)
    return market_refresh_task


async def cached_market_snapshot() -> tuple[float, str, list[dict[str, Any]]]:
    if market_cache and time.time() - market_cache[0] < MARKET_CACHE_SECONDS:
        return market_cache
    refresh = start_market_refresh()
    if market_cache:
        return market_cache
    try:
        return await asyncio.wait_for(
            asyncio.shield(refresh),
            timeout=MARKET_REFRESH_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=502, detail="全市场公开行情首次加载超时") from exc


async def warm_market_cache() -> None:
    try:
        await cached_market_snapshot()
    except HTTPException:
        return


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "A股板块资金流 API",
        "status": "ok",
        "endpoints": ["/health", "/api/market-snapshot", "/api/stock-metrics", "/api/industry-fund-flow", "/api/concept-fund-flow"],
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/market-snapshot")
async def market_snapshot() -> dict[str, Any]:
    cached_at, source, rows = await cached_market_snapshot()
    return {
        "source": source,
        "updated_at": datetime.fromtimestamp(cached_at, timezone.utc).isoformat(),
        "count": len(rows),
        "data": rows,
    }


@app.get("/api/stock-metrics")
async def stock_metrics(codes: str = Query(..., min_length=6, max_length=1000)) -> dict[str, Any]:
    clean_codes = sanitize_codes(codes.split(","))
    if not clean_codes:
        raise HTTPException(status_code=400, detail="请提供有效的六位股票代码")
    try:
        rows = await asyncio.wait_for(
            asyncio.to_thread(fetch_tencent_metrics, clean_codes),
            timeout=7,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"腾讯公开行情暂不可用：{type(exc).__name__}") from exc
    return {
        "source": "腾讯公开行情（AKShare 服务代理）",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(rows),
        "data": rows,
    }


@app.get("/api/industry-fund-flow")
async def industry_fund_flow(
    period: str = Query("即时", pattern="^(即时|3日排行|5日排行|10日排行|20日排行)$"),
) -> dict[str, Any]:
    rows = await cached_flow("industry", period)
    return {"source": "AKShare / 同花顺公开数据", "period": period, "count": len(rows), "data": rows}


@app.get("/api/concept-fund-flow")
async def concept_fund_flow(
    period: str = Query("即时", pattern="^(即时|3日排行|5日排行|10日排行|20日排行)$"),
) -> dict[str, Any]:
    rows = await cached_flow("concept", period)
    return {"source": "AKShare / 同花顺公开数据", "period": period, "count": len(rows), "data": rows}
