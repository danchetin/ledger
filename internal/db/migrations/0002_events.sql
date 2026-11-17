-- Transactional outbox: every state change writes an event in the same database
-- transaction, so an event exists if and only if the change committed.
--
-- tx_id is the writing transaction's id. Readers order by (tx_id, id) and only see
-- events whose tx_id is below the oldest transaction still running
-- (pg_snapshot_xmin). Ordering by id alone would lose events: ids are assigned at
-- INSERT, but transactions can commit in a different order, so a reader could move
-- its cursor past an id whose transaction had not committed yet.
CREATE TABLE events (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tx_id       xid8        NOT NULL DEFAULT pg_current_xact_id(),
    type        text        NOT NULL,
    resource_id uuid        NOT NULL,
    payload     jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX events_tx_id_id_idx ON events (tx_id, id);

CREATE TRIGGER events_append_only
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION ledger_forbid_change();
