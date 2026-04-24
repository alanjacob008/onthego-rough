import asyncio
import json
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websockets

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
]

OUTPUT_DIR = Path("output")


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


@dataclass
class SecondBucket:
    mids: list[float] = field(default_factory=list)
    last_bid: float | None = None
    last_ask: float | None = None
    update_count: int = 0
    first_exchange_ts: int | None = None
    last_exchange_ts: int | None = None
    first_local_ts: int | None = None
    last_local_ts: int | None = None


class MidCollector:
    def __init__(self, duration_seconds: int = 120):
        self.duration_seconds = duration_seconds
        self.start_ms = now_ms()
        self.end_ms = self.start_ms + duration_seconds * 1000
        self.buckets: dict[tuple[str, str, int], SecondBucket] = {}
        self.records: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []
        self.last_seen_ms: dict[tuple[str, str], int] = {}
        self.connected: dict[str, set[str]] = defaultdict(set)
        self.valid_mids: dict[str, set[str]] = defaultdict(set)
        self.parse_errors: dict[str, int] = defaultdict(int)
        self.reconnects: dict[str, int] = defaultdict(int)

    def push_update(
        self,
        exchange: str,
        symbol: str,
        bid: float,
        ask: float,
        exchange_ts_ms: int | None,
        source_channel: str,
    ) -> None:
        local_ts = now_ms()
        sec = local_ts // 1000

        if bid <= 0 or ask <= 0 or ask < bid:
            self.errors.append(
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "stage": "validate",
                    "error": "invalid_bid_ask",
                    "raw_payload": {
                        "bid": bid,
                        "ask": ask,
                        "exchange_ts": exchange_ts_ms,
                        "source_channel": source_channel,
                    },
                }
            )
            return

        mid = (bid + ask) / 2
        key = (exchange, symbol, sec)
        bucket = self.buckets.setdefault(key, SecondBucket())
        bucket.mids.append(mid)
        bucket.last_bid = bid
        bucket.last_ask = ask
        bucket.update_count += 1
        bucket.first_exchange_ts = exchange_ts_ms if bucket.first_exchange_ts is None else min(bucket.first_exchange_ts, exchange_ts_ms or bucket.first_exchange_ts)
        bucket.last_exchange_ts = exchange_ts_ms if bucket.last_exchange_ts is None else max(bucket.last_exchange_ts, exchange_ts_ms or bucket.last_exchange_ts)
        bucket.first_local_ts = local_ts if bucket.first_local_ts is None else min(bucket.first_local_ts, local_ts)
        bucket.last_local_ts = local_ts if bucket.last_local_ts is None else max(bucket.last_local_ts, local_ts)
        self.last_seen_ms[(exchange, symbol)] = local_ts
        self.connected[exchange].add(symbol)
        self.valid_mids[exchange].add(symbol)

    def finalize_second(self, sec: int) -> None:
        for exchange, native_symbols, channel in [
            ("binance", SYMBOLS, "bookTicker"),
            ("okx", [s.replace("USDT", "-USDT-SWAP") for s in SYMBOLS], "bbo-tbt"),
            ("bybit", SYMBOLS, "orderbook.1"),
        ]:
            for native in native_symbols:
                normalized = native.replace("-USDT-SWAP", "")
                key = (exchange, normalized, sec)
                bucket = self.buckets.get(key)
                if bucket and bucket.update_count > 0:
                    mids = sorted(bucket.mids)
                    rec = {
                        "ts_second": sec,
                        "exchange": exchange,
                        "symbol": normalized,
                        "native_symbol": native,
                        "best_bid_last": bucket.last_bid,
                        "best_ask_last": bucket.last_ask,
                        "mid_median": statistics.median(mids),
                        "mid_last": bucket.mids[-1],
                        "spread_bps_last": ((bucket.last_ask - bucket.last_bid) / ((bucket.last_ask + bucket.last_bid) / 2)) * 10000,
                        "updates_count": bucket.update_count,
                        "first_exchange_ts": bucket.first_exchange_ts,
                        "last_exchange_ts": bucket.last_exchange_ts,
                        "first_local_ts": bucket.first_local_ts,
                        "last_local_ts": bucket.last_local_ts,
                        "source_channel": channel,
                        "status": "observed",
                        "error_reason": None,
                    }
                else:
                    last = self.last_seen_ms.get((exchange, normalized))
                    stale = last is not None and ((sec * 1000) - last > 3000)
                    rec = {
                        "ts_second": sec,
                        "exchange": exchange,
                        "symbol": normalized,
                        "native_symbol": native,
                        "best_bid_last": None,
                        "best_ask_last": None,
                        "mid_median": None,
                        "mid_last": None,
                        "spread_bps_last": None,
                        "updates_count": 0,
                        "first_exchange_ts": None,
                        "last_exchange_ts": None,
                        "first_local_ts": None,
                        "last_local_ts": None,
                        "source_channel": channel,
                        "status": "stale" if stale else "missing",
                        "error_reason": None,
                    }
                self.records.append(rec)

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self._run_binance()),
            asyncio.create_task(self._run_okx()),
            asyncio.create_task(self._run_bybit()),
            asyncio.create_task(self._aggregator_loop()),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()

    async def _aggregator_loop(self) -> None:
        current_sec = self.start_ms // 1000
        while now_ms() < self.end_ms:
            await asyncio.sleep(0.25)
            sec = now_ms() // 1000
            while current_sec < sec:
                self.finalize_second(current_sec)
                current_sec += 1

        final_sec = now_ms() // 1000
        while current_sec <= final_sec:
            self.finalize_second(current_sec)
            current_sec += 1

    async def _run_binance(self) -> None:
        streams = "/".join(f"{s.lower()}@bookTicker" for s in SYMBOLS)
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        while now_ms() < self.end_ms:
            try:
                async with websockets.connect(url, ping_interval=15, ping_timeout=15) as ws:
                    async for msg in ws:
                        data = json.loads(msg).get("data", {})
                        s = data.get("s")
                        if not s:
                            continue
                        bid = float(data["b"])
                        ask = float(data["a"])
                        ts = int(data.get("E") or data.get("T") or now_ms())
                        self.push_update("binance", s, bid, ask, ts, "bookTicker")
                        if now_ms() >= self.end_ms:
                            break
            except Exception as e:
                self.reconnects["binance"] += 1
                self.errors.append({"exchange": "binance", "stage": "socket", "error": str(e), "raw_payload": {}})
                await asyncio.sleep(1)

    async def _run_okx(self) -> None:
        url = "wss://ws.okx.com:8443/ws/v5/public"
        args = [{"channel": "bbo-tbt", "instId": s.replace("USDT", "-USDT-SWAP")} for s in SYMBOLS]
        while now_ms() < self.end_ms:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    async for msg in ws:
                        obj = json.loads(msg)
                        if "event" in obj and obj["event"] == "error":
                            self.errors.append({"exchange": "okx", "stage": "subscribe", "error": obj.get("msg", "unknown"), "raw_payload": obj})
                            continue
                        arg = obj.get("arg", {})
                        if arg.get("channel") != "bbo-tbt":
                            continue
                        for item in obj.get("data", []):
                            inst = item.get("instId")
                            if not inst:
                                continue
                            symbol = inst.replace("-USDT-SWAP", "")
                            bid = ask = None
                            if item.get("bids") and item.get("asks"):
                                bid = float(item["bids"][0][0])
                                ask = float(item["asks"][0][0])
                            elif item.get("bidPx") and item.get("askPx"):
                                bid = float(item["bidPx"])
                                ask = float(item["askPx"])
                            if bid is None or ask is None:
                                self.parse_errors["okx"] += 1
                                continue
                            ts = int(item.get("ts", now_ms()))
                            self.push_update("okx", symbol, bid, ask, ts, "bbo-tbt")
                        if now_ms() >= self.end_ms:
                            break
            except Exception as e:
                self.reconnects["okx"] += 1
                self.errors.append({"exchange": "okx", "stage": "socket", "error": str(e), "raw_payload": {}})
                await asyncio.sleep(1)

    async def _run_bybit(self) -> None:
        url = "wss://stream.bybit.com/v5/public/linear"
        topics = [f"orderbook.1.{s}" for s in SYMBOLS]
        while now_ms() < self.end_ms:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": topics}))
                    async for msg in ws:
                        obj = json.loads(msg)
                        topic = obj.get("topic", "")
                        if not topic.startswith("orderbook.1."):
                            if obj.get("success") is False:
                                self.errors.append({"exchange": "bybit", "stage": "subscribe", "error": obj.get("ret_msg", "unknown"), "raw_payload": obj})
                            continue
                        symbol = topic.split(".")[-1]
                        data = obj.get("data", {})
                        bids = data.get("b") or []
                        asks = data.get("a") or []
                        if not bids or not asks:
                            self.parse_errors["bybit"] += 1
                            continue
                        bid = float(bids[0][0])
                        ask = float(asks[0][0])
                        ts = int(obj.get("ts") or data.get("ts") or now_ms())
                        self.push_update("bybit", symbol, bid, ask, ts, "orderbook.1")
                        if now_ms() >= self.end_ms:
                            break
            except Exception as e:
                self.reconnects["bybit"] += 1
                self.errors.append({"exchange": "bybit", "stage": "socket", "error": str(e), "raw_payload": {}})
                await asyncio.sleep(1)

    def write_outputs(self) -> None:
        OUTPUT_DIR.mkdir(exist_ok=True)
        run_started_at = iso_ms(self.start_ms)
        run_ended_at = iso_ms(now_ms())
        (OUTPUT_DIR / "mid_samples.json").write_text(
            json.dumps(
                {
                    "run_started_at": run_started_at,
                    "run_ended_at": run_ended_at,
                    "records": self.records,
                },
                indent=2,
            )
        )

        summaries = []
        tested_seconds = sorted({r["ts_second"] for r in self.records})
        seconds_tested = len(tested_seconds)
        for exchange in ["binance", "okx", "bybit"]:
            rows = [r for r in self.records if r["exchange"] == exchange]
            observed = [r for r in rows if r["status"] == "observed"]
            missing = [r for r in rows if r["status"] in {"missing", "stale"}]
            latencies = []
            for r in observed:
                if r["last_exchange_ts"] and r["last_local_ts"]:
                    latencies.append(r["last_local_ts"] - r["last_exchange_ts"])
            latencies.sort()
            p50 = latencies[len(latencies) // 2] if latencies else None
            p95 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))] if latencies else None
            updates_sum = sum(r["updates_count"] for r in observed)
            summaries.append(
                {
                    "exchange": exchange,
                    "markets_requested": len(SYMBOLS),
                    "markets_connected": len(self.connected[exchange]),
                    "markets_with_valid_mids": len(self.valid_mids[exchange]),
                    "markets_failed": len(SYMBOLS) - len(self.valid_mids[exchange]),
                    "seconds_tested": seconds_tested,
                    "expected_records": len(SYMBOLS) * seconds_tested,
                    "observed_records": len(observed),
                    "missing_records": len(missing),
                    "stale_markets_count": len([r for r in rows if r["status"] == "stale"]),
                    "parse_errors_count": self.parse_errors[exchange],
                    "reconnects_count": self.reconnects[exchange],
                    "latency_p50_ms": p50,
                    "latency_p95_ms": p95,
                    "updates_per_second_avg": (updates_sum / seconds_tested) if seconds_tested else 0,
                }
            )

        (OUTPUT_DIR / "run_summary.json").write_text(json.dumps(summaries, indent=2))
        (OUTPUT_DIR / "errors.json").write_text(json.dumps(self.errors, indent=2))

        symbol_mapping = {
            s.replace("USDT", "-USD"): {
                "binance": s,
                "okx": s.replace("USDT", "-USDT-SWAP"),
                "bybit": s,
            }
            for s in SYMBOLS
        }
        (OUTPUT_DIR / "symbol_mapping.json").write_text(json.dumps(symbol_mapping, indent=2))

        findings = f"""# 2-minute sample findings\n\n- Duration: {self.duration_seconds} seconds\n- Symbols per exchange: {len(SYMBOLS)} ({', '.join(SYMBOLS)})\n- Binance channel/url: `<symbol>@bookTicker` on `wss://fstream.binance.com/stream?streams=...`\n- OKX channel/url: `bbo-tbt` on `wss://ws.okx.com:8443/ws/v5/public`\n- Bybit channel/url: `orderbook.1.<symbol>` on `wss://stream.bybit.com/v5/public/linear`\n- Bucketing: local received second\n- Statuses emitted: observed / missing / stale\n\nSee `run_summary.json` for quality metrics and `errors.json` for caveats.\n"""
        (OUTPUT_DIR / "findings.md").write_text(findings)


def main() -> None:
    collector = MidCollector(duration_seconds=120)
    asyncio.run(collector.run())
    collector.write_outputs()


if __name__ == "__main__":
    main()
