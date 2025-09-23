\connect stl_data;

CREATE TABLE test_table(id SERIAL PRIMARY KEY);

CREATE TABLE IF NOT EXISTS snapshots (
    id SERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS historic_data (
    id SERIAL PRIMARY KEY,
    snapshot_id INT REFERENCES snapshots(id),
    old_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);