import asyncio
import math
import time
from typing import Any, Callable

import akshare as ak
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="A股板块资金流 API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

CACHE_SECONDS = 60
cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}


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


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "A股板块资金流 API",
        "status": "ok",
        "endpoints": ["/health", "/api/industry-fund-flow", "/api/concept-fund-flow"],
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
