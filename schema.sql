CREATE TABLE resources (
  id text PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 80),
  generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
  entitlement_key text
);

CREATE TABLE payments (
  idempotency_key text PRIMARY KEY CHECK (length(idempotency_key) BETWEEN 1 AND 80),
  resource_id text NOT NULL REFERENCES resources(id),
  generation integer NOT NULL CHECK (generation > 0),
  amount_minor integer NOT NULL CHECK (amount_minor > 0),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'completed', 'canceled')),
  completed_at timestamptz,
  canceled_at timestamptz,
  CHECK (
    (state = 'pending' AND completed_at IS NULL AND canceled_at IS NULL) OR
    (state = 'completed' AND completed_at IS NOT NULL AND canceled_at IS NULL) OR
    (state = 'canceled' AND canceled_at IS NOT NULL)
  )
);

ALTER TABLE resources ADD CONSTRAINT entitlement_payment
  FOREIGN KEY (entitlement_key) REFERENCES payments(idempotency_key);
CREATE INDEX payments_resource ON payments(resource_id);
