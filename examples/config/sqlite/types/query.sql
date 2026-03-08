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
