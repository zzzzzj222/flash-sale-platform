ALTER TABLE outbox DROP CHECK ck_destination;
ALTER TABLE outbox ADD CONSTRAINT ck_destination CHECK (destination IN ('RABBIT','WEBHOOK','REDIS'));
