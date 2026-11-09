-- Maximum run time of one attempt; NULL means no limit. Nullable with no default, so adding it is a
-- catalog-only change: existing rows aren't rewritten and the table isn't locked for long.
ALTER TABLE jobs ADD COLUMN timeout_seconds INT CHECK (timeout_seconds > 0);
