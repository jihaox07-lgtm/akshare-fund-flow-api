import math
from typing import Any, Callable


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _number(value: Any) -> float | None:
    if value is None or value == "" or value == "-":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def normalize_market_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in records:
        code = str(_first(row, "代码", "code") or "").strip()
        name = str(_first(row, "名称", "name") or "").strip()
        price = _number(_first(row, "最新价", "trade"))
        change_pct = _number(_first(row, "涨跌幅", "changepercent"))
        turnover = _number(_first(row, "成交额", "amount"))
        if not code or not name or price is None or change_pct is None or turnover is None:
            continue
        normalized.append({
            "code": code,
            "name": name,
            "price": price,
            "change_pct": change_pct,
            "turnover": turnover,
            "turnover_rate": _number(_first(row, "换手率", "turnoverratio")),
            "volume_ratio": _number(_first(row, "量比", "volumeratio")),
        })
    return normalized


def fetch_market_records(
    providers: list[tuple[str, Callable[[], list[dict[str, Any]]]]],
    minimum_rows: int = 1_000,
) -> tuple[str, list[dict[str, Any]]]:
    failures: list[str] = []
    for source, provider in providers:
        try:
            rows = normalize_market_records(provider())
            if len(rows) < minimum_rows:
                raise ValueError(f"有效行情仅 {len(rows)} 条")
            return source, rows
        except Exception as exc:
            failures.append(f"{source}: {type(exc).__name__}")
    raise RuntimeError("；".join(failures) or "所有全市场行情源均不可用")
