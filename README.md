# A股板块资金流 API

基于 FastAPI 与 AKShare，提供 A 股全市场行情、行业和概念板块资金流 JSON 接口。

## 接口

- `GET /health`
- `GET /api/market-snapshot`
- `GET /api/industry-fund-flow?period=即时`
- `GET /api/concept-fund-flow?period=即时`

`period` 可选：`即时`、`3日排行`、`5日排行`、`10日排行`、`20日排行`。

全市场行情启动后会自动预热并缓存十分钟；优先使用新浪财经公开行情，失败后自动切换至东方财富公开行情。

数据采集自公开页面，可能因上游页面调整而中断，仅用于市场研究。
