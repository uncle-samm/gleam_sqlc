CREATE TABLE types_sqlite (
    c_integer INTEGER,
    c_real REAL,
    c_text TEXT,
    c_blob BLOB
);


-- Declared types with NUMERIC affinity: SQLite stores a number as an
-- INTEGER or a REAL, and text that looks like a number as a number.
CREATE TABLE types_sqlite_affinity (
    id INTEGER PRIMARY KEY,
    c_numeric NUMERIC NOT NULL DEFAULT 0,
    c_decimal DECIMAL(10, 2),
    c_boolean BOOLEAN NOT NULL DEFAULT 0,
    c_date DATE,
    c_datetime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    c_timestamp TIMESTAMP,
    c_varchar VARCHAR(20),
    c_bigint BIGINT,
    c_double DOUBLE PRECISION,
    c_untyped
);
