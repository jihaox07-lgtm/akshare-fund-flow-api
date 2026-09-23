from datetime import datetime
import re
from typing import Any
from urllib.request import Request, urlopen


MAX_CODES = 120


def sanitize_codes(codes: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in codes:
        code = raw.strip()
        if not re.fullmatch(r"\d{6}", code) or code in seen:
            continue
        seen.add(code)
        result.append(code)
        if len(result) >= MAX_CODES:
            break
    return result


def _number(value: str | None) -> float | None:
    try:
        parsed = float(value or "")
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _source_time(value: str | None) -> str | None:
    if not value or not re.fullmatch(r"\d{14}", value):
        return None
    try:
        parsed = datetime.strptime(value, "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%dT%H:%M:%S+08:00")


def parse_tencent_metrics(raw: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in re.findall(r'v_[^=]+="([^"]*)"', raw):
        fields = payload.split("~")
        code = fields[2].strip() if len(fields) > 2 else ""
        turnover_rate = _number(fields[38] if len(fields) > 38 else None)
        volume_ratio = _number(fields[49] if len(fields) > 49 else None)
        source_updated_at = _source_time(fields[30] if len(fields) > 30 else None)
        if not re.fullmatch(r"\d{6}", code) or turnover_rate is None or source_updated_at is None:
            continue
        rows.append({
            "code": code,
            "turnover_rate": turnover_rate,
            "volume_ratio": volume_ratio or 0.0,
            "source_updated_at": source_updated_at,
        })
    return rows


def _market_symbol(code: str) -> str:
    if code.startswith("6"):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    return f"bj{code}"


def fetch_tencent_metrics(codes: list[str], timeout: float = 5.0) -> list[dict[str, Any]]:
    clean_codes = sanitize_codes(codes)
    if not clean_codes:
        return []
    symbols = ",".join(_market_symbol(code) for code in clean_codes)
    request = Request(
        f"https://qt.gtimg.cn/q={symbols}",
        headers={"Accept": "text/plain,*/*", "Referer": "https://gu.qq.com/"},
    )
    with urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("gb18030", errors="replace")
    return parse_tencent_metrics(raw)
