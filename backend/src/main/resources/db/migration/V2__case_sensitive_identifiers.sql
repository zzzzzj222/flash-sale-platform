-- Business user and client identifiers must preserve case on every MySQL installation.
ALTER TABLE outbox DROP FOREIGN KEY fk_outbox_request;
ALTER TABLE user_quotas DROP FOREIGN KEY fk_quota_activity;
ALTER TABLE reservations DROP FOREIGN KEY fk_reservation_activity;

ALTER TABLE activities CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE reservations CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE user_quotas CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
ALTER TABLE outbox CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;

ALTER TABLE reservations ADD CONSTRAINT fk_reservation_activity
    FOREIGN KEY (tenant_id, activity_id) REFERENCES activities (tenant_id, id);
ALTER TABLE user_quotas ADD CONSTRAINT fk_quota_activity
    FOREIGN KEY (tenant_id, activity_id) REFERENCES activities (tenant_id, id);
ALTER TABLE outbox ADD CONSTRAINT fk_outbox_request
    FOREIGN KEY (tenant_id, request_id) REFERENCES reservations (tenant_id, request_id);
