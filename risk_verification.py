"""Conservative, source-attributed A-share risk screening.

`pass` only describes the specified check and its source coverage, never investment
safety. An incomplete source may establish a positive risk but never a negative.
"""

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
import math
import re
import threading
import time
from typing import Any, Callable

import pandas as pd
import requests


SHANGHAI = timezone(timedelta(hours=8))
CHECK_IDS = ("listing", "trading", "special_treatment", "key_event", "unlock")
UNLOCK_HARD_LIMIT_PCT = 18.0
SOURCE_URLS = {
    "listing": "https://quote.eastmoney.com/center/gridlist.html#hs_a_board",
    "trading": "https://data.eastmoney.com/tfpxx/",
    "special_treatment": "https://quote.eastmoney.com/center/gridlist.html#st_board",
    "key_event": "https://data.eastmoney.com/notices/",
    "unlock": "https://data.eastmoney.com/dxf/",
}
SOURCE_NAMES = {
    "listing": "东方财富个股资料及交易所退市名单",
    "trading": "东方财富停复牌公告及个股行情",
    "special_treatment": "东方财富个股实时简称",
    "key_event": "东方财富上市公司公告",
    "unlock": "东方财富限售解禁批次、个股总股本及新浪交易日历",
}
_RISK_TITLE = re.compile(
    r"退市风险警示|终止上市|被立案调查|立案告知|行政处罚决定|重大诉讼|重大仲裁|"
    r"被申请破产|债务逾期|无法表示意见|否定意见|财务造假|涉嫌信息披露违法"
)
_RESOLUTION_TITLE = re.compile(r"撤销|解除|不予|终结|结案|风险消除")
_INDEPENDENT_RISK_TITLE = re.compile(r"被立案调查|立案告知|涉嫌信息披露违法|财务造假|债务逾期|被申请破产")


@dataclass
class Coverage:
    rows: list[dict[str, Any]]
    complete: bool
    reason: str = ""


@dataclass(frozen=True)
class FetchFailure:
    reason: str


def parse_codes(raw: str) -> list[str]:
    codes = raw.split(",")
    if not 1 <= len(codes) <= 30 or any(not re.fullmatch(r"\d{6}", code) for code in codes):
        raise ValueError("codes 须为 1–30 个以逗号分隔的规范六位股票代码")
    if len(set(codes)) != len(codes):
        raise ValueError("codes 不允许重复")
    return codes


def unavailable_reports(codes: list[str], as_of: date, reason: str) -> list[dict[str, Any]]:
    """Return explicit unknown evidence when a whole verification batch fails."""
    retrieved = datetime.now(timezone.utc).isoformat()
    return [{
        "code": code,
        "asOf": as_of.isoformat(),
        "windowEnd": None,
        "overall": "pending",
        "checks": [{
            "id": check_id,
            "state": "unknown",
            "source": SOURCE_NAMES[check_id],
            "sourceUrl": SOURCE_URLS[check_id],
            "sourceUpdatedAt": None,
            "retrievedAt": retrieved,
            "reason": reason,
        } for check_id in CHECK_IDS],
        "unlockRatio": None,
        "unlockRatioBasis": None,
    } for code in codes]


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value[:10]) if "-" in value else datetime.strptime(value[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _positive_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _risk_title_hit(title: str) -> bool:
    return bool(_INDEPENDENT_RISK_TITLE.search(title) or
                _RISK_TITLE.search(title) and not _RESOLUTION_TITLE.search(title))


class EastmoneyRiskSource:
    """Bounded public requests. Incomplete pagination is explicit in Coverage."""

    DATACENTER = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.request_gap_seconds = 0.35
        self._request_lock = threading.Lock()
        self._last_request = 0.0

    def _get_json(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        with self._request_lock:
            remaining = self.request_gap_seconds - (time.monotonic() - self._last_request)
            if remaining > 0:
                time.sleep(remaining)
            self._last_request = time.monotonic()
        response = self.session.get(url, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("上游不是 JSON 对象")
        return data

    def _datacenter_pages(self, params: dict[str, Any], *, max_pages: int = 5) -> Coverage:
        rows: list[dict[str, Any]] = []
        count = pages = None
        for page in range(1, max_pages + 1):
            try:
                payload = self._get_json(self.DATACENTER, {**params, "pageNumber": page})
                result = payload.get("result")
                if payload.get("success") is not True or not isinstance(result, dict):
                    return Coverage(rows, False, "上游返回失败或缺少 result")
                batch = result.get("data")
                if not isinstance(batch, list) or any(not isinstance(row, dict) for row in batch):
                    return Coverage(rows, False, "上游数据结构无效")
                current_count, current_pages = result.get("count"), result.get("pages")
                if not isinstance(current_count, int) or not isinstance(current_pages, int) or current_count < 0 or current_pages < 0:
                    return Coverage(rows, False, "上游缺少有效分页总数")
                if count is None:
                    count, pages = current_count, current_pages
                elif (count, pages) != (current_count, current_pages):
                    return Coverage(rows, False, "上游分页总数变化")
                if count == 0 and pages == 0 and not batch:
                    return Coverage([], True)
                if page > pages or (page < pages and not batch):
                    return Coverage(rows, False, "上游分页缺页")
                if page < pages and len(batch) != params.get("pageSize"):
                    return Coverage(rows, False, "上游非末页行数不足")
                rows.extend(batch)
                if page >= pages:
                    return Coverage(rows, len(rows) == count, "" if len(rows) == count else "上游行数与分页总数不符")
            except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
                return Coverage(rows, False, f"上游抓取失败：{type(exc).__name__}")
        return Coverage(rows, False, "上游分页超过安全上限")

    def load_calendar(self) -> list[date]:
        # AKShare's documented Sina decoder is used with a local HTTP timeout.
        from akshare.tool.trade_date_hist import hk_js_decode, py_mini_racer

        response = self.session.get("https://finance.sina.com.cn/realstock/company/klc_td_sh.txt", timeout=5)
        response.raise_for_status()
        encoded = response.text.split("=", 1)[1].split(";", 1)[0].replace('"', "")
        js = py_mini_racer.MiniRacer()
        js.eval(hk_js_decode)
        values = js.call("d", encoded)
        parsed = {_date(value) for value in values}
        return sorted(value for value in parsed if value is not None)

    def load_profile(self, code: str) -> dict[str, Any]:
        market = 1 if code.startswith("6") else 0 if code.startswith(("0", "3")) else None
        if market is None:
            raise ValueError("北交所及其他号段个股资料尚未核实")
        payload = self._get_json("https://push2.eastmoney.com/api/qt/stock/get", {
            "secid": f"{market}.{code}", "fields": "f57,f58,f84,f86,f189,f47", "fltt": 2, "invt": 2,
        })
        data = payload.get("data")
        if not isinstance(data, dict) or str(data.get("f57")) != code:
            raise ValueError("个股资料缺失或代码不匹配")
        quote_date = None
        quote_time = None
        try:
            quote_time = datetime.fromtimestamp(int(data["f86"]), SHANGHAI)
            quote_date = quote_time.date().isoformat()
        except (ValueError, TypeError, KeyError, OverflowError):
            pass
        return {
            "code": code,
            "name": data.get("f58") if isinstance(data.get("f58"), str) else None,
            "listed_on": _date(str(data.get("f189", ""))).isoformat() if _date(str(data.get("f189", ""))) else None,
            "quote_date": quote_date,
            "volume": _positive_number(data.get("f47")),
            "total_shares": _positive_number(data.get("f84")),
            "source_updated_at": quote_time.isoformat() if quote_time else None,
        }

    def load_suspensions(self, as_of: date) -> Coverage:
        raw = self._datacenter_pages({
            "reportName": "RPT_CUSTOM_SUSPEND_DATA_INTERFACE",
            "columns": "ALL", "source": "WEB", "client": "WEB", "pageSize": 500,
            "sortColumns": "SUSPEND_START_DATE", "sortTypes": "-1",
            "filter": f'(MARKET="全部")(DATETIME=\'{as_of.isoformat()}\')',
        })
        parsed = []
        complete = raw.complete
        for row in raw.rows:
            code, start = row.get("SECURITY_CODE"), _date(row.get("SUSPEND_START_DATE"))
            if not isinstance(code, str) or not re.fullmatch(r"\d{6}", code) or start is None:
                complete = False
                continue
            end = _date(row.get("SUSPEND_END_TIME"))
            resume = _date(row.get("PREDICT_RESUME_DATE"))
            parsed.append({"code": code, "start": start.isoformat(), "end": end.isoformat() if end else None,
                           "resume": resume.isoformat() if resume else None, "reason": str(row.get("SUSPEND_REASON") or "停牌公告")})
        return Coverage(parsed, complete, raw.reason or ("停复牌字段不完整" if not complete else ""))

    def load_delisted(self, code: str) -> Coverage:
        if code.startswith("6"):
            return self._load_sh_delisted()
        if code.startswith(("0", "3")):
            return self._load_sz_delisted()
        return Coverage([], False, "该市场退市名单尚未核实")

    def _load_sh_delisted(self) -> Coverage:
        params = {
            "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L", "isPagination": "true", "STOCK_CODE": "",
            "CSRC_CODE": "", "REG_PROVINCE": "", "STOCK_TYPE": "1,2,8", "COMPANY_STATUS": "3",
            "type": "inParams", "pageHelp.cacheSize": 1, "pageHelp.beginPage": 1,
            "pageHelp.pageSize": 500, "pageHelp.pageNo": 1, "pageHelp.endPage": 1,
        }
        try:
            response = self.session.get("https://query.sse.com.cn/commonQuery.do", params=params,
                                        headers={"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"}, timeout=5)
            response.raise_for_status()
            payload = response.json()
            raw = payload.get("result")
            if not isinstance(raw, list):
                return Coverage([], False, "上交所退市名单结构未确认")
            rows = [{"code": str(row.get("COMPANY_CODE")), "delisted_on": str(row.get("DELIST_DATE", ""))[:10]}
                    for row in raw if isinstance(row, dict)]
            return Coverage(rows, False, "上交所接口分页总量未核实；仅命中项可用于排除")
        except (requests.RequestException, ValueError) as exc:
            return Coverage([], False, f"上交所退市名单不可用：{type(exc).__name__}")

    def _load_sz_delisted(self) -> Coverage:
        try:
            response = self.session.get("https://www.szse.cn/api/report/ShowReport", params={
                "SHOWTYPE": "xlsx", "CATALOGID": "1793_ssgs", "TABKEY": "tab2",
            }, timeout=5)
            response.raise_for_status()
            frame = pd.read_excel(BytesIO(response.content))
            if not {"证券代码", "终止上市日期"}.issubset(frame.columns):
                return Coverage([], False, "深交所退市名单字段变化")
            rows = [{"code": str(row["证券代码"]).split(".")[0].zfill(6),
                     "delisted_on": _date(row["终止上市日期"]).isoformat() if _date(row["终止上市日期"]) else ""}
                    for _, row in frame.iterrows()]
            return Coverage(rows, True)
        except (requests.RequestException, ValueError, ImportError) as exc:
            return Coverage([], False, f"深交所退市名单不可用：{type(exc).__name__}")

    def load_announcements(self, code: str, start: date, end: date) -> Coverage:
        rows: list[dict[str, Any]] = []
        previous_date: date | None = None
        for page in range(1, 6):
            try:
                payload = self._get_json("https://np-anotice-stock.eastmoney.com/api/security/ann", {
                    "sr": -1, "page_size": 50, "page_index": page, "ann_type": "A",
                    "client_source": "web", "stock_list": code, "f_node": 0, "s_node": 0,
                })
                data = payload.get("data")
                if payload.get("success") != 1 or not isinstance(data, dict):
                    return Coverage(rows, False, "公告上游返回失败")
                batch = data.get("list")
                total = data.get("total_hits")
                if not isinstance(batch, list) or not isinstance(total, int):
                    return Coverage(rows, False, "公告分页元数据缺失")
                for item in batch:
                    if not isinstance(item, dict) or not isinstance(item.get("title"), str):
                        return Coverage(rows, False, "公告字段不完整")
                    published = _date(item.get("notice_date"))
                    codes = item.get("codes")
                    if published is None or not isinstance(codes, list) or code not in [c.get("stock_code") for c in codes if isinstance(c, dict)]:
                        return Coverage(rows, False, "公告日期或股票代码不匹配")
                    if previous_date is not None and published > previous_date:
                        return Coverage(rows, False, "公告排序不稳定")
                    previous_date = published
                    if start <= published <= end:
                        art = item.get("art_code")
                        rows.append({"code": code, "date": published.isoformat(), "title": item["title"],
                                     "url": f"https://pdf.dfcfw.com/pdf/H2_{art}_1.pdf" if isinstance(art, str) and re.fullmatch(r"AN\d+", art) else None})
                if previous_date is not None and previous_date < start:
                    return Coverage(rows, True)
                if len(batch) < 50:
                    complete = page * 50 >= total or (page - 1) * 50 + len(batch) == total
                    return Coverage(rows, complete, "" if complete else "公告分页提前结束")
            except (requests.RequestException, ValueError, TypeError) as exc:
                return Coverage(rows, False, f"公告抓取失败：{type(exc).__name__}")
        return Coverage(rows, False, "公告页数超过安全上限")

    def load_unlocks(self, code: str) -> Coverage:
        raw = self._datacenter_pages({
            "reportName": "RPT_LIFT_STAGE", "filter": f'(SECURITY_CODE="{code}")',
            "columns": "SECURITY_CODE,SECURITY_NAME_ABBR,FREE_DATE,CURRENT_FREE_SHARES,ABLE_FREE_SHARES,LIFT_MARKET_CAP,FREE_RATIO,NEW,B20_ADJCHRATE,A20_ADJCHRATE,FREE_SHARES_TYPE,TOTAL_RATIO,NON_FREE_SHARES,BATCH_HOLDER_NUM",
            "pageSize": 200, "source": "WEB", "client": "WEB", "sortColumns": "FREE_DATE", "sortTypes": "-1",
        })
        rows = []
        complete = raw.complete
        for item in raw.rows:
            when = _date(item.get("FREE_DATE"))
            shares_wan = _positive_number(item.get("ABLE_FREE_SHARES"))
            if item.get("SECURITY_CODE") != code or when is None:
                complete = False
                continue
            # Eastmoney raw field is in 10,000-share units; AKShare multiplies by 10,000.
            rows.append({"code": code, "date": when.isoformat(),
                         "shares": shares_wan * 10_000 if shares_wan is not None else None})
        return Coverage(rows, complete, raw.reason or ("解禁字段不完整" if not complete else ""))


class RiskVerifier:
    def __init__(self, source: Any | None = None, *, today: date | None = None,
                 today_provider: Callable[[], date] | None = None) -> None:
        self.source = source or EastmoneyRiskSource()
        self._today_provider = today_provider or (lambda: today or datetime.now(SHANGHAI).date())
        self._semaphore = asyncio.Semaphore(4)
        self.source_timeout_seconds = 9.0
        self._cache: dict[tuple[Any, ...], tuple[float, Any]] = {}
        self._inflight: dict[tuple[Any, ...], asyncio.Task[Any]] = {}
        self._started: set[tuple[Any, ...]] = set()

    async def _load(self, key: tuple[Any, ...], function: Any, *args: Any) -> Any:
        cached = self._cache.get(key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        if key not in self._inflight:
            async def run() -> Any:
                try:
                    async with self._semaphore:
                        self._started.add(key)
                        try:
                            try:
                                value = await asyncio.to_thread(function, *args)
                            except Exception as exc:
                                value = FetchFailure(f"{type(exc).__name__}；该来源未能完成核验")
                            ttl = 5 if isinstance(value, FetchFailure) or isinstance(value, Coverage) and not value.complete else 90
                            self._cache[key] = (time.monotonic() + ttl, value)
                            return value
                        finally:
                            self._started.discard(key)
                finally:
                    if self._inflight.get(key) is asyncio.current_task():
                        self._inflight.pop(key, None)

            self._inflight[key] = asyncio.create_task(run())
        task = self._inflight[key]
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=self.source_timeout_seconds)
        except asyncio.TimeoutError:
            if key not in self._started and not task.done():
                task.cancel()
                if self._inflight.get(key) is task:
                    self._inflight.pop(key, None)
            raise

    async def verify(self, codes: list[str], as_of: date) -> list[dict[str, Any]]:
        calendar, suspensions = await asyncio.gather(
            self._safe(("calendar", as_of.isoformat()), self.source.load_calendar),
            self._safe(("suspensions", as_of.isoformat()), self.source.load_suspensions, as_of),
        )
        dates = sorted({day for day in calendar if isinstance(day, date)}) if isinstance(calendar, list) else []
        future = [day for day in dates if day > as_of]
        window_end = future[4] if len(future) >= 5 else None
        latest_trade_day = max((day for day in dates if day <= self._today_provider()), default=None)
        reports = await asyncio.gather(*(self._verify_one(code, as_of, window_end, latest_trade_day, suspensions, calendar) for code in codes))
        return list(reports)

    async def _safe(self, key: tuple[Any, ...], function: Any, *args: Any) -> Any:
        try:
            return await self._load(key, function, *args)
        except Exception as exc:
            return FetchFailure(f"{type(exc).__name__}；该来源未能完成核验")

    async def _verify_one(self, code: str, as_of: date, window_end: date | None,
                          latest_trade_day: date | None, suspensions: Any, calendar: Any) -> dict[str, Any]:
        profile, delisted, announcements, unlocks = await asyncio.gather(
            self._safe(("profile", code, as_of.isoformat()), self.source.load_profile, code),
            self._safe(("delisted", code[0]), self.source.load_delisted, code),
            self._safe(("announcements", code, as_of.isoformat()), self.source.load_announcements, code, as_of - timedelta(days=30), as_of),
            self._safe(("unlocks", code, as_of.isoformat()), self.source.load_unlocks, code),
        )
        retrieved = datetime.now(timezone.utc).isoformat()
        evidence: list[dict[str, Any]] = []

        def add(check_id: str, state: str, reason: str, *, source_updated_at: str | None = None,
                url: str | None = None) -> None:
            evidence.append({"id": check_id, "state": state, "source": SOURCE_NAMES[check_id],
                             "sourceUrl": url or SOURCE_URLS[check_id], "sourceUpdatedAt": source_updated_at,
                             "retrievedAt": retrieved, "reason": reason})

        profile_ok = isinstance(profile, dict) and profile.get("code") == code
        quote_fresh = profile_ok and profile.get("quote_date") == as_of.isoformat()
        list_date = _date(profile.get("listed_on")) if profile_ok else None
        delist_hit = None
        if isinstance(delisted, Coverage):
            delist_hit = next((row for row in delisted.rows if row.get("code") == code and
                               (when := _date(row.get("delisted_on"))) and when <= as_of), None)
        if delist_hit:
            delisted_on = _date(delist_hit["delisted_on"])
            exchange_url = ("https://www.sse.com.cn/assortment/stock/list/delisting/" if code.startswith("6")
                            else "https://www.szse.cn/market/stock/suspend/index.html")
            add("listing", "risk", f"交易所记录终止上市日期 {delisted_on}",
                url=exchange_url)
        elif list_date and list_date > as_of:
            add("listing", "risk", f"上市日期 {list_date} 晚于核验日")
        elif quote_fresh and list_date and list_date <= as_of and (
            isinstance(delisted, Coverage) and delisted.complete or _positive_number(profile.get("volume"))
        ):
            basis = "核验日有实际成交" if _positive_number(profile.get("volume")) else "交易所退市名单覆盖完整"
            add("listing", "pass", f"上市于 {list_date}，个股行情日期与核验日一致，{basis}",
                source_updated_at=profile.get("source_updated_at"))
        else:
            detail = f"；个股资料错误：{profile.reason}" if isinstance(profile, FetchFailure) else ""
            if isinstance(delisted, (Coverage, FetchFailure)) and delisted.reason:
                detail += f"；退市名单：{delisted.reason}"
            add("listing", "unknown", f"上市日期、当日行情或退市名单证据不足，不能认定在市{detail}",
                source_updated_at=profile.get("source_updated_at") if profile_ok else None)

        active_suspension = None
        if isinstance(suspensions, Coverage):
            for row in suspensions.rows:
                start, end, resume = _date(row.get("start")), _date(row.get("end")), _date(row.get("resume"))
                if row.get("code") == code and start and start <= as_of and (end is None or end >= as_of):
                    active_suspension = row
                    break
        if active_suspension:
            add("trading", "risk", f"停牌自 {active_suspension['start']} 起：{active_suspension.get('reason') or '原因未注明'}")
        elif quote_fresh and _positive_number(profile.get("volume")):
            add("trading", "pass", "核验日有正成交量的当日行情；未以零成交量反推停牌",
                source_updated_at=profile.get("source_updated_at"))
        else:
            detail = f"；停牌源错误：{suspensions.reason}" if isinstance(suspensions, (Coverage, FetchFailure)) and suspensions.reason else ""
            add("trading", "unknown", f"没有可证明当日正常交易的成交记录；停牌名单未命中不足以判定正常{detail}",
                source_updated_at=profile.get("source_updated_at") if profile_ok else None)

        name = profile.get("name") if profile_ok else None
        if quote_fresh and isinstance(name, str) and re.match(r"^\*?ST", name, re.I):
            add("special_treatment", "risk", f"核验日股票简称为 {name}", source_updated_at=profile.get("source_updated_at"))
        elif quote_fresh and isinstance(name, str) and name.strip():
            add("special_treatment", "pass", f"核验日股票简称 {name} 未带 ST/*ST 前缀；仅覆盖简称标识",
                source_updated_at=profile.get("source_updated_at"))
        else:
            add("special_treatment", "unknown", "缺少与核验日一致的股票简称，不能排除 ST/*ST")

        event = None
        if isinstance(announcements, Coverage):
            event = next((row for row in announcements.rows if row.get("code") == code and
                          (when := _date(row.get("date"))) and as_of - timedelta(days=30) <= when <= as_of and
                          _risk_title_hit(str(row.get("title", "")))), None)
        if event:
            add("key_event", "risk", f"近 30 日公告（日期 {event.get('date')}）命中重大风险标题：{event['title']}",
                url=event.get("url"))
        else:
            detail = announcements.reason if isinstance(announcements, (Coverage, FetchFailure)) and announcements.reason else "公告标题检索不覆盖全部持续性或未公告风险"
            add("key_event", "unknown", f"近 30 日及已知仍生效事项未获完整排除证据：{detail}")

        ratio = None
        shares_total = _positive_number(profile.get("total_shares")) if profile_ok else None
        if window_end is None or latest_trade_day != as_of:
            detail = f"；日历源错误：{calendar.reason}" if isinstance(calendar, FetchFailure) else ""
            add("unlock", "unknown", f"交易日历缺少未来五个交易日，或核验日并非最新交易日{detail}")
        elif shares_total is None or not quote_fresh:
            add("unlock", "unknown", "总股本或核验日行情缺失，无法计算以总股本为分母的百分数")
        elif not isinstance(unlocks, Coverage):
            detail = unlocks.reason if isinstance(unlocks, FetchFailure) else "来源未返回有效批次"
            add("unlock", "unknown", f"解禁批次上游不可用：{detail}")
        else:
            relevant = [row for row in unlocks.rows if row.get("code") == code and
                        (when := _date(row.get("date"))) and as_of < when <= window_end]
            known = [_positive_number(row.get("shares")) for row in relevant]
            lower_bound = sum(value for value in known if value is not None) / shares_total * 100
            if known and all(value is not None for value in known):
                ratio = lower_bound
            elif not relevant and unlocks.complete:
                ratio = 0.0
            if lower_bound >= UNLOCK_HARD_LIMIT_PCT:
                if unlocks.complete and all(value is not None for value in known):
                    add("unlock", "risk", f"未来五交易日解禁合计占总股本 {lower_bound:.15g}%，达到 {UNLOCK_HARD_LIMIT_PCT:g}% 排除阈值")
                else:
                    add("unlock", "risk", f"已取得批次至少占总股本 {lower_bound:.15g}%，即使其他批次不完整仍达到排除阈值")
                    ratio = None
            elif not unlocks.complete or any(value is None for value in known):
                add("unlock", "unknown", f"解禁批次或数量不完整：{unlocks.reason or '缺少有效股数'}")
                ratio = None
            elif ratio is not None:
                add("unlock", "pass", f"未来五交易日已公布解禁合计占总股本 {ratio:.15g}%，未达到 {UNLOCK_HARD_LIMIT_PCT:g}% 阈值")
            else:
                add("unlock", "unknown", "解禁数量单位或总股本分母不可核验")

        states = {item["state"] for item in evidence}
        overall = "rejected" if "risk" in states else "verified" if states == {"pass"} else "pending"
        return {"code": code, "asOf": as_of.isoformat(), "windowEnd": window_end.isoformat() if window_end else None,
                "overall": overall, "checks": evidence, "unlockRatio": ratio,
                "unlockRatioBasis": "total_shares" if ratio is not None else None}
