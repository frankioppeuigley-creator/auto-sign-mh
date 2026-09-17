# AGENTS.md

## What this is

Automated daily sign-in / coupon / lottery script for the "美好超市" (meihao) platform. Runs via Docker with Redis-backed scheduling. All times are CST (UTC+8).

## Entry points

- **`run_proxy.py`** — Primary. Redis-based scheduler with proxy pool, time-window batching, retry logic, and daily HTML email reports.
- **`run_all.py`** — Legacy simpler mode. No proxy, no Redis, sequential execution, runs once at 00:01. Uses `schedule` library.
- **`run_proxy_copy.py`** — Old snapshot of `run_proxy.py`. Not maintained; do not edit or reference.

## Dependencies & infra

- Python 3.11, deps: `requests`, `redis`, `pandas`
- **Redis is required** for `run_proxy.py`. Docker Compose starts both `redis:7-alpine` and the app.
- No tests, no linting, no type checking, no CI. Changes are verified by running in Docker.

## Running locally

```bash
docker-compose up -d          # build + start app + redis
docker-compose logs -f app    # tail logs
docker-compose exec app bash  # shell into container
```

## CLI flags (run_proxy.py)

| Flag | What it does |
|------|-------------|
| *(no flag)* | Start the heartbeat loop (default production mode) |
| `--once` | Generate schedule, execute all batches immediately, send report, exit |
| `--status` | Print queue status and schedule table |
| `--generate` | Generate today's schedule only |
| `--retry` | Generate retry schedule for failed accounts |
| `--add <phones...>` | Hot-add accounts to config.json + Redis pending |
| `--delete <phones...>` | Remove accounts from config.json + all Redis queues + schedule batches |
| `--clear` | `FLUSHDB` — wipes all Redis data |

## Config files

- **`config.json`** — Runtime config with real credentials. Git-ignored.
- **`config`** — Empty template with user_agents list. Tracked in git. Copy to `config.json` and fill in accounts + email to get started.

## Architecture: scheduling flow

1. On first heartbeat after 06:00, `generate_schedule()` shuffles accounts into random batches (3-8 each), assigns random execution times across the 06:00-17:00 window, writes to Redis ZSET `schedule`.
2. Heartbeat loop (every 30s) checks for due batches and executes them with `ThreadPoolExecutor` (max 3 concurrent).
3. Each account gets a **fresh proxy** fetched inside its thread (proxy TTL is ~1 min, so can't batch-fetch).
4. Failures go to `retry_queue`. At 18:00, retry batches are scheduled into 18:00-23:59 window. Max 3 retries per account before `giveup`.
5. Daily HTML report emailed when all done or at 23:59 (forced). Uses `SETNX` to prevent duplicate sends.
6. Sleep window: 00:00-06:00 — no tasks execute.
7. New accounts added to `config.json` are auto-detected every 5 min and appended to the schedule.

## API details (meihao)

- Base: `https://meihao.v3.api.meihaocvs.com/api`, data: `.../data/api`
- Auth: SMS code login (phone + uuid), returns token used as `Authorization` header.
- Request signing: `make_sign()` sorts keys, concatenates `key=|={val}&`, replaces `"` with `'`, `true`/`false` with `1`/`0`, then MD5.
- Every payload gets `app_version`, `noce` (nonce), `timetmp`, and `sign` auto-injected by `prepare_payload()`.
- Lottery coupon IDs are formatted as hex: `format(cid, 'x')`.

## Gotchas

- `config.json` is git-ignored but the container mounts it as a volume. Edit on the host, not inside the container.
- Proxy credentials (`PROXY_USER`/`PROXY_PASS`) and the proxy API key are hardcoded in `run_proxy.py` — not in config.json.
- `APP_VERSION = "4053"` is hardcoded. If the meihao app updates, this must change or requests will fail silently.
- The `noce` field in `prepare_payload()` is intentionally misspelled (it's what the API expects).
- Logs rotate daily under `/app/logs/` (overridable via `LOG_DIR` env var), retained 7 days.
- `--clear` runs `FLUSHDB` — affects all keys in the selected Redis DB, not just this app's keys.
- `--delete` modifies both `config.json` and Redis in one operation. Partial failure (e.g. config updated but Redis down) leaves them out of sync.
