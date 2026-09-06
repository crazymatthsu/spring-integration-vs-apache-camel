CREATE TABLE IF NOT EXISTS orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cl_ord_id       TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    side            TEXT    NOT NULL,
    order_qty       REAL    NOT NULL,
    ord_type        TEXT    NOT NULL,
    price           REAL,
    transact_time   TEXT    NOT NULL,
    sender_comp_id  TEXT    NOT NULL,
    target_comp_id  TEXT    NOT NULL,
    msg_seq_num     INTEGER NOT NULL,
    raw_message     TEXT    NOT NULL,
    inserted_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Redelivered records (at-least-once) are ignored by the INSERT OR IGNORE statement thanks to this index.
CREATE UNIQUE INDEX IF NOT EXISTS ux_orders_sender_cl_ord_id ON orders (sender_comp_id, cl_ord_id);
