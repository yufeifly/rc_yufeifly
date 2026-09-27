#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import statistics
import time
import urllib.request


def submit(base_url: str, token: str, index: int) -> float:
    body = json.dumps(
        {
            "idempotency_key": f"benchmark-{index}",
            "provider": "benchmark",
            "event_type": "benchmark.event",
            "payload": {"sequence": index},
        }
    ).encode()
    request = urllib.request.Request(
        f"{base_url}/v1/notifications",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    started = time.perf_counter()
    with urllib.request.urlopen(request, timeout=5) as response:
        if response.status != 202:
            raise RuntimeError(f"unexpected status {response.status}")
        response.read()
    return time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:18281")
    parser.add_argument("--token", default="benchmark-token")
    parser.add_argument("--requests", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--min-rps", type=float, default=50)
    parser.add_argument("--max-p95-ms", type=float, default=200)
    args = parser.parse_args()

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        latencies = list(
            pool.map(
                lambda index: submit(args.url, args.token, index),
                range(args.requests),
            )
        )
    elapsed = time.perf_counter() - started
    throughput = args.requests / elapsed
    p95 = statistics.quantiles(latencies, n=100, method="inclusive")[94] * 1000
    print(
        json.dumps(
            {
                "requests": args.requests,
                "concurrency": args.concurrency,
                "throughput_rps": round(throughput, 2),
                "p95_ms": round(p95, 2),
            }
        )
    )
    if throughput < args.min_rps:
        raise SystemExit(f"throughput {throughput:.2f} below {args.min_rps:.2f} req/s")
    if p95 > args.max_p95_ms:
        raise SystemExit(f"p95 {p95:.2f}ms above {args.max_p95_ms:.2f}ms")


if __name__ == "__main__":
    main()
