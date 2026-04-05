# PROD-Parser (JSONL-first)

PROD Telegram parser core with:

- JSONL storage (no PostgreSQL)
- custom internal message index (`internal_id`) independent from Telegram IDs
- fast in-memory inverted search index with persistent snapshot
- online topic detection across different users (reply + lexical + time window)
- relations graph (reply / mention / co-topic / temporal adjacency / context overlap)
- toxicity scoring and top-words analytics
- weighted smart-words ranking (TF-IDF-like) for combot-style insights
- content popularity analytics (files/mentions/URLs) by chat/user/topic
- media deduplication by file hash with canonical media linking
- per-user activity score with message share, consistency and lexical quality
- model-ready context packets + chat snapshot export
- participant-level JSON export with profile + relations + topics + recent messages
- built-in web dashboard for user snapshot visualization
- one-shot full analytics command for all chats (`analyze-all`)
- output JSON schema generation for exported formats (`schema`)
- dual-channel logging: pretty console + JSON `.log` file
- OpenAI-compatible LLM enrichment (single/per-task routing + Dig Deeper mode)
- mindmap-friendly graph export (`mindmap.nodes` + `mindmap.edges`)
- Telegram MTProto ingestion (`gotd/td`) with auth/session/floodwait/rate limit
- real avatar download with persistent cache (not only avatar refs)

## Environment config

Copy `.env.example` to `.env` and fill your values.

```bash
cp .env.example .env
```

CLI loads `.env` automatically on startup.

## Quick start

### 1) Ingest normalized JSONL

```bash
go run ./cmd/prod-parser ingest --state ./data --input ./examples/messages.sample.jsonl
```

### 2) Search

```bash
go run ./cmd/prod-parser search --state ./data --query "релиз баг fix" --limit 20
```

### 3) Export snapshot for API/static site

```bash
go run ./cmd/prod-parser snapshot \
  --state ./data --account acc1 --chat 10001 \
  --llm \
  --llm-base-url "$LLM_BASE_URL" \
  --llm-api-key "$LLM_API_KEY" \
  --llm-model "gpt-4o-mini" \
  --llm-routing per-task \
  --llm-persona-model "gpt-4.1-mini" \
  --llm-topic-model "gpt-4o-mini" \
  --llm-relation-model "gpt-4o-mini" \
  --llm-mindmap-model "gpt-4o-mini" \
  --llm-model-info-url "/models/info" \
  --llm-input-tokens 32000 \
  --llm-output-tokens 2048 \
  --llm-batch-messages 250 \
  --llm-dig-deeper \
  --llm-dig-model "o3"
```

`--llm-model-info-url` and `--llm-model-info-file` allow dynamic per-model token capacities (`max_input_tokens`, `max_output_tokens`).
If metadata is unavailable, defaults are used (`--llm-input-tokens`, `--llm-output-tokens`, `--llm-safety-tokens`, `--llm-min-output-tokens`).

### 4) Export per-participant JSON files

```bash
go run ./cmd/prod-parser participants \
  --state ./data --account acc1 --chat 10001 \
  --llm --llm-dig-deeper
```

### 5) Build complete analytics in one command

```bash
go run ./cmd/prod-parser analyze-all \
  --state ./data --account acc1 --chat 10001 \
  --profile full --participants --with-schema \
  --llm --llm-dig-deeper
```

Without `--account`/`--chat`, command scans all chats under `data/jsonl/*/*` and exports:

- full chat snapshot (`data/exports/<account>/<chat>_snapshot.json`)
- per-user snapshots (`data/exports/<account>/<chat>_users/user_<id>.json`)
- output schema files (`data/exports/schemas/*.schema.json`)

### 6) Generate output schema only

```bash
go run ./cmd/prod-parser schema --out ./data/exports/schemas --pretty
```

### 7) Query messages in time range

```bash
go run ./cmd/prod-parser range \
  --state ./data --account acc1 --chat 10001 \
  --from 2026-04-01T00:00:00Z --to 2026-04-05T23:59:59Z \
  --weekday 1 --yearday 120 --limit 2000
```

### 8) Launch user dashboard

```bash
go run ./cmd/prod-parser dashboard --state ./data --host 127.0.0.1 --port 8787
```

Then open the printed URL in browser.

The dashboard reads user snapshots from `data/exports/<account>/<chat>_users/user_<id>.json`,
shows message activity timelines, lexical pressure, relation edges, and recent messages with filter.

### 9) Fetch from Telegram API directly

Build with Telegram support tag first:

```bash
go run -tags telegram ./cmd/prod-parser tg-fetch \
  --state ./data --account acc1 \
  --api-id "$TG_API_ID" --api-hash "$TG_API_HASH" \
  --phone "$TG_PHONE" --peer @my_channel --limit 5000 \
  --file-max-messages 5000 \
  --fetch-bio --fetch-avatars \
  --avatar-dir ./data/avatars \
  --avatar-cache ./data/avatars/cache.json \
  --avatar-big \
  --topic-mode embedding --embedding-model text-embedding-3-large
```

Persistent polling mode with command authorization:

```bash
go run -tags telegram ./cmd/prod-parser tg-fetch \
  --state ./data --account acc1 \
  --api-id "$TG_API_ID" --api-hash "$TG_API_HASH" \
  --phone "$TG_PHONE" --peer @my_channel \
  --poll --poll-interval-ms 5000 \
  --poll-with-backfill \
  --max-info \
  --file-max-messages 5000 \
  --cmd-policy admins \
  --search-limit 7 \
  --owner-id 123456789 \
  --cmd-prefix /
```

`--poll-with-backfill` first ingests history, then continues with live polling in one process.
`--max-info` enables `--fetch-bio`, `--fetch-avatars`, and `--with-raw` with default avatar path under `--state`.
`--file-max-messages` splits JSONL into segments (`messages_000001.jsonl`, `messages_000002.jsonl`, ...).

Supported command policies:

- `owner` (default): only account owner
- `admins`: owner + chat/channel admins
- `users`: any user in chat
- `ids`: only IDs from `--cmd-ids "123,456"`

Polling commands accepted in chat:

- `/status`
- `/search <query>`
- `/ask <question>`
- `/pause`
- `/resume`
- `/stop`

`/stop` sends a completion report to owner (`--owner-id` or `--owner-username`) when configured.

Without `-tags telegram`, command returns explicit disabled message.

```bash
go run ./cmd/prod-parser tg-fetch \
  --state ./data --account acc1 \
  --api-id "$TG_API_ID" --api-hash "$TG_API_HASH" \
  --phone "$TG_PHONE" --peer @my_channel --limit 5000 --fetch-bio
```

This writes:

- `data/exports/<account>/<chat>_snapshot.json`
- downloaded avatars into `data/avatars/` and cache state into `data/avatars/cache.json`

## Input JSONL shape

Each line should be one normalized message object:

```json
{
  "account_id": "acc1",
  "chat_id": 10001,
  "message_id": 12345,
  "date": "2026-04-04T21:00:00Z",
  "from_user_id": 777,
  "from_username": "username",
  "from_display_name": "Name Surname",
  "from_bio": "optional profile about",
  "text": "release candidate готов",
  "reply_to_msg_id": 12340,
  "mentions_user_ids": [888],
  "media_type": "photo",
  "has_voice": false
}
```

## Why this design

- JSONL is append-friendly and easy to archive.
- `internal_id` avoids relying on Telegram ordering quirks and lets you build stable references.
- Offset pointers allow O(1) random reads from JSONL.
- Inverted token index gives fast search while data remains in JSONL files.
- Snapshot format is API-friendly and static-site-friendly.

## Main packages

- `internal/storage/jsonl`: append/read JSONL with byte offsets
- `internal/index`: dedup + internal IDs + inverted index
- `internal/analysis`: stats, smart words, toxicity, topics, relations, personas, context packets
- `internal/pipeline`: orchestration layer
- `internal/llm`: OpenAI-compatible LLM client and enrichment logic
- `internal/telegramingest`: optional Telegram API ingestion (build-tagged)

## Notes

- Current toxicity model is lexical heuristic; replace later with classifier/LLM scorer.
- Current topic model is online lightweight heuristic; you can add embeddings re-clustering batch job.
- Inverted index is in-memory + persisted snapshot. For very large scale, shard per chat/time bucket.
- `participants` export is ready for static-site/API serving as separate JSON per user.
- Activity score combines message share, meaningful-word quality, engagement and consistency.
- With `--llm`, snapshots include `summary`, `llm`, and `mindmap` blocks.
- Logs are written both to console (readable format) and file (`./logs/prod-parser.log`) in JSON format.
