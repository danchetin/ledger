-- Double-entry ledger schema.
--
-- Invariants enforced by the database itself (the service checks them too):
--   * every transaction's entries sum to zero per currency (deferred constraint trigger)
--   * every transaction has at least two entries (deferred constraint trigger)
--   * an entry's currency equals its account's currency (composite foreign key)
--   * an account without overdraft never has negative available balance (CHECK)
--   * transactions and entries are append-only (triggers reject UPDATE/DELETE)

CREATE TABLE accounts (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text        NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    currency        char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    allow_overdraft boolean     NOT NULL DEFAULT false,
    balance         bigint      NOT NULL DEFAULT 0,
    held            bigint      NOT NULL DEFAULT 0 CHECK (held >= 0),
    metadata        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT accounts_id_currency_key UNIQUE (id, currency),
    CONSTRAINT accounts_available_non_negative CHECK (allow_overdraft OR balance - held >= 0)
);

CREATE TABLE transactions (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    kind        text        NOT NULL CHECK (kind IN ('transfer', 'capture', 'reversal')),
    description text        NOT NULL DEFAULT '' CHECK (length(description) <= 500),
    metadata    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    reverses_id uuid        UNIQUE REFERENCES transactions (id),
    created_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT transactions_reversal_link CHECK ((kind = 'reversal') = (reverses_id IS NOT NULL))
);

CREATE TABLE entries (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id uuid        NOT NULL REFERENCES transactions (id),
    account_id     uuid        NOT NULL,
    currency       char(3)     NOT NULL,
    amount         bigint      NOT NULL CHECK (amount <> 0),
    balance_after  bigint      NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT entries_account_currency_fkey
        FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency),
    CONSTRAINT entries_one_leg_per_account UNIQUE (transaction_id, account_id)
);

CREATE INDEX entries_account_id_id_idx ON entries (account_id, id DESC);

CREATE TABLE holds (
    id                     uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id             uuid        NOT NULL,
    destination_account_id uuid        NOT NULL,
    currency               char(3)     NOT NULL,
    amount                 bigint      NOT NULL CHECK (amount > 0),
    captured_amount        bigint      NOT NULL DEFAULT 0,
    status                 text        NOT NULL DEFAULT 'active'
                                       CHECK (status IN ('active', 'captured', 'voided', 'expired')),
    description            text        NOT NULL DEFAULT '' CHECK (length(description) <= 500),
    metadata               jsonb       NOT NULL DEFAULT '{}'::jsonb,
    expires_at             timestamptz NOT NULL,
    transaction_id         uuid        UNIQUE REFERENCES transactions (id),
    created_at             timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT holds_account_currency_fkey
        FOREIGN KEY (account_id, currency) REFERENCES accounts (id, currency),
    CONSTRAINT holds_destination_currency_fkey
        FOREIGN KEY (destination_account_id, currency) REFERENCES accounts (id, currency),
    CONSTRAINT holds_distinct_accounts CHECK (account_id <> destination_account_id),
    CONSTRAINT holds_captured_amount CHECK (captured_amount BETWEEN 0 AND amount),
    CONSTRAINT holds_capture_link CHECK ((status = 'captured') = (transaction_id IS NOT NULL))
);

CREATE INDEX holds_active_expires_at_idx ON holds (expires_at) WHERE status = 'active';
CREATE INDEX holds_active_account_idx ON holds (account_id) WHERE status = 'active';

-- Stored responses for Idempotency-Key. The body is kept as bytes so a replay is
-- byte-for-byte identical to the original response.
CREATE TABLE idempotency_keys (
    key           text        PRIMARY KEY CHECK (length(key) BETWEEN 1 AND 255),
    request_hash  bytea       NOT NULL,
    status_code   integer,
    response_body bytea,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);

-- Every transaction must balance per currency. Checked at COMMIT, after all legs exist.
CREATE FUNCTION ledger_check_transaction_balanced() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    bad record;
BEGIN
    SELECT currency, sum(amount) AS total INTO bad
    FROM entries
    WHERE transaction_id = NEW.transaction_id
    GROUP BY currency
    HAVING sum(amount) <> 0
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'transaction % is unbalanced in %: %', NEW.transaction_id, bad.currency, bad.total
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER entries_balanced
    AFTER INSERT ON entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_check_transaction_balanced();

CREATE FUNCTION ledger_check_transaction_has_entries() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (SELECT count(*) FROM entries WHERE transaction_id = NEW.id) < 2 THEN
        RAISE EXCEPTION 'transaction % has fewer than two entries', NEW.id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER transactions_have_entries
    AFTER INSERT ON transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_check_transaction_has_entries();

CREATE FUNCTION ledger_forbid_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END
$$;

CREATE TRIGGER entries_append_only
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION ledger_forbid_change();

CREATE TRIGGER transactions_append_only
    BEFORE UPDATE OR DELETE ON transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_forbid_change();
