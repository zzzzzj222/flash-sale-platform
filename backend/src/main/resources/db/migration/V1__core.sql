CREATE TABLE activities (
    tenant_id VARCHAR(64) NOT NULL,
    id CHAR(36) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    title VARCHAR(120) NOT NULL,
    total_stock INT NOT NULL,
    available_stock INT NOT NULL,
    limit_per_user INT NOT NULL,
    hold_seconds INT NOT NULL,
    queue_timeout_seconds INT NOT NULL,
    starts_at_ms BIGINT NOT NULL,
    ends_at_ms BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT ck_stock CHECK (available_stock >= 0 AND available_stock <= total_stock),
    CONSTRAINT ck_activity CHECK (total_stock > 0 AND limit_per_user > 0 AND hold_seconds > 0 AND ends_at_ms > starts_at_ms)
) ENGINE=InnoDB;

CREATE TABLE reservations (
    tenant_id VARCHAR(64) NOT NULL,
    request_id CHAR(36) NOT NULL,
    activity_id CHAR(36) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    quantity INT NOT NULL,
    state VARCHAR(20) NOT NULL,
    reason VARCHAR(40),
    order_id VARCHAR(128),
    queue_deadline_ms BIGINT NOT NULL,
    expires_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, request_id),
    KEY ix_expiry (state, expires_at_ms),
    KEY ix_queued (state, queue_deadline_ms),
    KEY ix_user (tenant_id, user_id, created_at_ms),
    CONSTRAINT fk_reservation_activity FOREIGN KEY (tenant_id, activity_id) REFERENCES activities (tenant_id, id),
    CONSTRAINT ck_quantity CHECK (quantity > 0),
    CONSTRAINT ck_state CHECK (state IN ('QUEUED','RESERVED','CONFIRMED','RELEASED','EXPIRED','REJECTED'))
) ENGINE=InnoDB;

CREATE TABLE user_quotas (
    tenant_id VARCHAR(64) NOT NULL,
    activity_id CHAR(36) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    used_quantity INT NOT NULL,
    PRIMARY KEY (tenant_id, activity_id, user_id),
    CONSTRAINT fk_quota_activity FOREIGN KEY (tenant_id, activity_id) REFERENCES activities (tenant_id, id),
    CONSTRAINT ck_quota CHECK (used_quantity >= 0)
) ENGINE=InnoDB;

CREATE TABLE outbox (
    id CHAR(36) NOT NULL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    request_id CHAR(36) NOT NULL,
    destination VARCHAR(16) NOT NULL,
    event_key VARCHAR(40) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'READY',
    attempts INT NOT NULL DEFAULT 0,
    available_at_ms BIGINT NOT NULL,
    locked_until_ms BIGINT NOT NULL DEFAULT 0,
    lease_owner CHAR(36),
    last_error VARCHAR(160),
    created_at_ms BIGINT NOT NULL,
    KEY ix_dispatch (status, available_at_ms, locked_until_ms),
    KEY ix_tenant_status (tenant_id, status, created_at_ms),
    UNIQUE KEY uk_event (tenant_id, request_id, event_key),
    CONSTRAINT fk_outbox_request FOREIGN KEY (tenant_id, request_id) REFERENCES reservations (tenant_id, request_id),
    CONSTRAINT ck_destination CHECK (destination IN ('RABBIT','WEBHOOK')),
    CONSTRAINT ck_outbox_state CHECK (status IN ('READY','IN_FLIGHT','DELIVERED','DEAD'))
) ENGINE=InnoDB;
