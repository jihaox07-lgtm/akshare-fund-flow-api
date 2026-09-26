# A股板块资金流 API

基于 FastAPI 与 AKShare，提供 A 股全市场行情、行业和概念板块资金流 JSON 接口。

## 接口

- `GET /health`
- `GET /api/market-snapshot`
- `GET /api/stock-risk?codes=600000,000001&as_of=2026-09-24`
- `GET /api/industry-fund-flow?period=即时`
- `GET /api/concept-fund-flow?period=即时`

`period` 可选：`即时`、`3日排行`、`5日排行`、`10日排行`、`20日排行`。

全市场行情启动后会自动预热并缓存十分钟；优先使用新浪财经公开行情，失败后自动切换至东方财富公开行情。

数据采集自公开页面，可能因上游页面调整而中断，仅用于市场研究。

## 逐项风险证据

`stock-risk` 每次接收 1–30 个不重复的六位代码，`as_of` 使用不晚于当日的真实 `YYYY-MM-DD` 日期。响应为 `{as_of,count,data}`；每个 `data` 项包含 `code`、`asOf`、`windowEnd`、`overall`、`checks`、`unlockRatio`、`unlockRatioBasis`。`checks` 固定依次为 `listing`、`trading`、`special_treatment`、`key_event`、`unlock`，每项有 `state`（`pass`、`risk`、`unknown`）、`source`、`sourceUrl`、`sourceUpdatedAt`、`retrievedAt` 和 `reason`。`overall` 为任一 `risk` 则 `rejected`，全数 `pass` 才 `verified`，否则 `pending`。

解禁窗口是 `as_of` 后五个交易日（`windowEnd` 为第五日），`unlockRatio` 单位是**占总股本的百分数**，例如 `19` 表示 19%。完整且可核验的空解禁批次返回 `0`；分页、股数单位、总股本或交易日历不完整时返回 `null`，不以 `0` 代替。项目硬排除阈值为 18%。分页不完整但已知批次单独超过阈值时可判 `risk`，比例仍为 `null`，原因会给出已知下界。

公告标题检索仅覆盖最近 30 日可取得的已披露公告，无法排除未公告或更早且仍生效的事项，因此无风险标题命中时 `key_event` 保持 `unknown`；当前服务不会仅凭这些公开源声称一只股票完全 `verified`。行情日不匹配、停牌名单空而缺成交证据、上游失败、源字段变化也都保持 `unknown`。缓存仅为进程内短时缓存，不跨 Render 重启保存。
