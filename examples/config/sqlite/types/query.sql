-- name: InsertSqliteTypes :exec
INSERT INTO types_sqlite (c_integer, c_real, c_text, c_blob)
VALUES (?, ?, ?, ?);

-- name: GetSqliteTypes :one
SELECT c_integer, c_real, c_text, c_blob
FROM types_sqlite
LIMIT 1;

-- name: GetSqliteTypesCnt :one
SELECT
    c_integer,
    c_real,
    c_text,
    c_blob,
    count(*) AS cnt
FROM types_sqlite
GROUP BY c_integer, c_real, c_text, c_blob
LIMIT 1;

-- name: InsertSqliteTypesBatch :copyfrom
INSERT INTO types_sqlite (c_integer, c_real, c_text, c_blob)
VALUES (?, ?, ?, ?);

-- name: GetSqliteFunctions :one
SELECT
    MAX(c_integer) AS max_integer,
    MAX(c_real) AS max_real,
    MAX(c_text) AS max_text
FROM types_sqlite;

-- name: DeleteAllSqliteTypes :exec
DELETE FROM types_sqlite;

/* Declared types with NUMERIC affinity */

-- name: InsertSqliteAffinity :one
INSERT INTO types_sqlite_affinity
(
    c_numeric,
    c_decimal,
    c_boolean,
    c_date,
    c_timestamp,
    c_varchar,
    c_bigint,
    c_double,
    c_untyped
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetSqliteAffinity :one
SELECT * FROM types_sqlite_affinity
WHERE id = ?;

-- name: GetSqliteAffinityAggregates :one
SELECT
    count(*) AS cnt,
    sum(c_numeric) AS sum_numeric,
    avg(c_bigint) AS avg_bigint,
    max(c_datetime) AS max_datetime,
    total(c_bigint) AS total_bigint
FROM types_sqlite_affinity;

-- name: GetSqliteAffinityByIds :many
SELECT id FROM types_sqlite_affinity
WHERE
    c_boolean = sqlc.arg(flag)
    AND id IN (sqlc.slice(ids))
    AND c_numeric >= sqlc.arg(min_numeric)
    AND (c_varchar = sqlc.arg(label) OR sqlc.arg(label) = '?1 -- not a parameter')
ORDER BY id;

-- name: DeleteAllSqliteAffinity :exec
DELETE FROM types_sqlite_affinity;
