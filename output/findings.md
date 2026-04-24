# 2-minute sample findings

- Duration: 120 seconds
- Symbols per exchange: 5 (BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT, DOGEUSDT)
- Binance channel/url: `<symbol>@bookTicker` on `wss://fstream.binance.com/stream?streams=...`
- OKX channel/url: `bbo-tbt` on `wss://ws.okx.com:8443/ws/v5/public`
- Bybit channel/url: `orderbook.1.<symbol>` on `wss://stream.bybit.com/v5/public/linear`
- Bucketing: local received second
- Statuses emitted: observed / missing / stale

See `run_summary.json` for quality metrics and `errors.json` for caveats.
