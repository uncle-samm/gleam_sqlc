-- name: GetAuthor :one
SELECT * FROM authors WHERE name = $1 LIMIT 1;

-- name: ListAuthors :many
SELECT * FROM authors ORDER BY name;

-- name: CreateAuthor :one
INSERT INTO authors (id, name, bio) VALUES ($1, $2, $3) RETURNING *;

-- name: DeleteAuthor :exec
DELETE FROM authors WHERE name = $1;

-- name: UpdateAuthors :execrows
UPDATE authors SET bio = $1 WHERE bio IS NOT NULL;
