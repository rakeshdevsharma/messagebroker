CREATE TABLE topics (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE messages (
    id          BIGSERIAL PRIMARY KEY,
    topic_id    BIGINT NOT NULL REFERENCES topics(id),
    content     BYTEA NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_messages_created_at ON messages(created_at);

CREATE TABLE consumer_group (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Referenced by CreateSubscription when no consumer group is specified.
INSERT INTO consumer_group (name) VALUES ('default');

CREATE TABLE consumer (
    id                  BIGSERIAL PRIMARY KEY,
    consumer_group_id   BIGINT NOT NULL REFERENCES consumer_group(id),
    name                TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (consumer_group_id, name)
);

CREATE TABLE subscriptions (
    topic_id            BIGINT NOT NULL REFERENCES topics(id),
    consumer_group_id   BIGINT NOT NULL REFERENCES consumer_group(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic_id, consumer_group_id)
);

CREATE TABLE message_queue (
    id                  BIGSERIAL PRIMARY KEY,
    message_id          BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    consumer_group_id   BIGINT NOT NULL REFERENCES consumer_group(id),
    consumer_id         BIGINT REFERENCES consumer(id),
    state               TEXT NOT NULL CHECK (state IN ('ready', 'unacked')),
    delivery_count      INT NOT NULL DEFAULT 0,
    lease_expires_at    TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (message_id, consumer_group_id)
);
CREATE INDEX idx_message_queue_claim ON message_queue (consumer_group_id, state, message_id);
CREATE INDEX idx_message_queue_lease ON message_queue (state, lease_expires_at) WHERE state = 'unacked';
