CREATE INDEX ix_outbox_destination_dispatch ON outbox (destination, status, available_at_ms, locked_until_ms);
