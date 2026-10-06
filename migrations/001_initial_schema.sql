CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id UUID NOT NULL,
    currency CHAR(3) NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT wallets_balance_non_negative
        CHECK (balance >= 0),

    CONSTRAINT wallets_version_positive
        CHECK (version >= 1),

    CONSTRAINT wallets_player_currency_unique
        UNIQUE (player_id, currency)
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    provider_id UUID NOT NULL,
    external_transaction_id VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,

    player_id UUID NOT NULL,
    wallet_id UUID NOT NULL,
    round_id VARCHAR(255),
    game_id VARCHAR(255),

    kind VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,

    amount BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,

    reference_external_transaction_id VARCHAR(255),
    resulting_balance BIGINT,

    failure_code VARCHAR(64),

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT wager_transactions_amount_positive
        CHECK (amount > 0)
);

CREATE UNIQUE INDEX wager_transactions_provider_external_id_unique
    ON wager_transactions (provider_id, external_transaction_id);

CREATE UNIQUE INDEX wager_transactions_provider_idempotency_key_unique
    ON wager_transactions (provider_id, idempotency_key);

CREATE INDEX wager_transactions_wallet_id_idx
    ON wager_transactions (wallet_id);

CREATE INDEX wager_transactions_reference_external_id_idx
    ON wager_transactions (provider_id, reference_external_transaction_id);


CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    wallet_id UUID NOT NULL
        REFERENCES wallets(id),

    transaction_id UUID NOT NULL
        REFERENCES wager_transactions(id),

    direction VARCHAR(16) NOT NULL,

    amount BIGINT NOT NULL,
    balance_before BIGINT NOT NULL,
    balance_after BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT wallet_ledger_entries_amount_positive
        CHECK (amount > 0),

    CONSTRAINT wallet_ledger_entries_balance_before_non_negative
        CHECK (balance_before >= 0),

    CONSTRAINT wallet_ledger_entries_balance_after_non_negative
        CHECK (balance_after >= 0),

    CONSTRAINT wallet_ledger_entries_transaction_unique
        UNIQUE (wallet_id, transaction_id)
);

CREATE INDEX wallet_ledger_entries_wallet_id_idx
    ON wallet_ledger_entries (wallet_id);


CREATE TABLE inbox_messages (
    consumer_name VARCHAR(100) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,

    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    PRIMARY KEY (consumer_name, message_id)
);


CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    event_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL,

    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,

    CONSTRAINT outbox_events_attempts_non_negative
        CHECK (attempts >= 0)
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (next_attempt_at, occurred_at)
    WHERE published_at IS NULL;
