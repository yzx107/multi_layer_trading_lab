# ifind_events

Purpose: normalized event and fundamental-enrichment records from iFind.

This table is an enrichment input for event-driven research. It should not be a hidden dependency for the base demo pipeline.

Primary key:

- `event_id`

Required fields:

- `event_id`
- `security_id`
- `symbol`
- `market`
- `event_date`
- `event_ts`
- `event_type`
- `event_title`
- `event_source`
- `data_source`
- `source_dataset`
- `source_run_id`
- `ingested_at`

Optional fields:

- `importance`

Usage rules:

- Keep iFind credentials in environment variables only.
- Treat overlapping Tushare/iFind fields as cross-check candidates.
- Preserve source timestamps and ingestion timestamps separately.
- Do not use future-published event fields in historical labels.

Manual export import:

- Use `write-ifind-events-template` to create an importable CSV template.
- Minimal English headers: `id`, `symbol`, `date`, `type`, `title`, `importance`.
- Common iFind Chinese headers are accepted: `事件ID`, `证券代码`, `公告日期`, `事件类型`, `公告标题`, `重要性`.
- Imported terminal files are marked as `data_source=ifind_real_file`.
