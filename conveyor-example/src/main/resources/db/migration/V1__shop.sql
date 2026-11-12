-- The shop's own tables. Its V1 lives in flyway_schema_history; conveyor's V1 (the jobs table) is
-- tracked apart in conveyor_schema_history, so the two never collide.
CREATE TABLE products (
    id    bigserial PRIMARY KEY,
    name  text NOT NULL,
    stock int  NOT NULL CHECK (stock >= 0)
);

CREATE TABLE orders (
    id                   bigserial   PRIMARY KEY,
    product_id           bigint      NOT NULL REFERENCES products (id),
    quantity             int         NOT NULL CHECK (quantity > 0),
    email                text        NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    -- set by the order.confirm job once the email went out
    confirmation_sent_at timestamptz
);

INSERT INTO products (name, stock) VALUES ('keyboard', 3), ('mouse', 0);
