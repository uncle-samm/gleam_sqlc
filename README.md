# sqlc-gen-gleam

A [sqlc](https://sqlc.dev) WASM plugin that generates type-safe [Gleam](https://gleam.run) code for PostgreSQL and SQLite.

- **PostgreSQL** via [postgleam](https://hex.pm/packages/postgleam)
- **SQLite** via [glite](https://hex.pm/packages/glite)

## Quick Start

### 1. Configure sqlc

```yaml
# sqlc.yaml
version: "2"
plugins:
  - name: gleam
    wasm:
      url: https://github.com/uncle-samm/gleam_sqlc/releases/download/v0.1.0/sqlc-gen-gleam.wasm
      sha256: "<sha256 from release>"
sql:
  - schema: "schema.sql"
    queries: "query.sql"
    engine: "postgresql"  # or "sqlite"
    codegen:
      - plugin: gleam
        out: "src/generated"
```

### 2. Write SQL

```sql
-- schema.sql
CREATE TABLE authors (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    bio TEXT
);

-- query.sql

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
```

### 3. Generate

```sh
sqlc generate
```

### 4. Use the generated code

```gleam
import generated/query
import postgleam

pub fn main() {
  use conn <- postgleam.connect("postgres://localhost/mydb")

  // :one - returns Result(Row, Error)
  let assert Ok(author) = query.get_author(conn, "Alice")

  // :many - returns Result(Response(Row), Error)
  let assert Ok(response) = query.list_authors(conn)
  let authors = response.rows

  // :exec - returns Result(Response(Nil), Error)
  let assert Ok(_) = query.delete_author(conn, "Bob")

  // :execrows - returns Result(Int, Error)
  let assert Ok(count) = query.update_authors(conn, option.Some("Updated bio"))
}
```

## Generated Code

Given the SQL above, sqlc-gen-gleam produces:

**`models.gleam`**
```gleam
import gleam/option.{type Option}

pub type Author {
  Author(id: Int, name: String, bio: Option(String))
}
```

**`query.gleam`**
```gleam
import gleam/option.{type Option}
import postgleam
import postgleam/decode

const get_author_sql = "SELECT id, name, bio FROM authors WHERE name = $1 LIMIT 1"

pub type GetAuthorRow {
  GetAuthorRow(id: Int, name: String, bio: Option(String))
}

pub fn get_author(conn, name: String) {
  let decoder = {
    use id <- decode.element(0, decode.int)
    use name <- decode.element(1, decode.text)
    use bio <- decode.element(2, decode.optional(decode.text))
    decode.success(GetAuthorRow(id:, name:, bio:))
  }
  postgleam.query_one(conn, get_author_sql, [postgleam.text(name)], decoder)
}

// ... more generated functions
```

## Supported Query Annotations

| Annotation | Description | Return Type |
|------------|-------------|-------------|
| `:one` | Single row | `Result(Row, Error)` |
| `:many` | Multiple rows | `Result(Response(Row), Error)` |
| `:exec` | Execute, no result | `Result(Response(Nil), Error)` |
| `:execrows` | Execute, return row count | `Result(Int, Error)` |
| `:copyfrom` | Batch insert | `Result(Nil, Error)` |

## Features

- **Automatic type mapping** - PostgreSQL/SQLite types map to idiomatic Gleam types
- **Nullable handling** - Nullable columns become `Option(T)`
- **Parameter ergonomics** - 0 params: no args, 1 param: named arg, 2+ params: `Params` type
- **Enums** - PostgreSQL enums generate Gleam custom types with `from_string`/`to_string` helpers
- **`sqlc.arg('name')`** - Custom parameter names
- **`sqlc.narg('name')`** - Force-nullable parameters
- **`sqlc.embed(table)`** - Embed table types in JOIN results
- **`sqlc.slice('name')`** - Dynamic IN-list expansion (SQLite)
- **Array parameters** - `ANY($1::TYPE[])` for PostgreSQL
- **Type overrides** - Override column types via plugin options

## Plugin Options

```yaml
codegen:
  - plugin: gleam
    out: "src/generated"
    options:
      overrides:
        - column: "GetAuthor:bio"
          gleamType:
            type: "String"
            notNull: true
```

Override matching supports `QueryName:ColumnName` or `*:ColumnName` patterns.

## Type Mapping

### PostgreSQL

| PostgreSQL | Gleam | Param | Decoder |
|------------|-------|-------|---------|
| `bool` | `Bool` | `postgleam.bool` | `decode.bool` |
| `int2/int4/serial` | `Int` | `postgleam.int` | `decode.int` |
| `int8/bigserial` | `Int` | `postgleam.int` | `decode.int` |
| `float4/float8` | `Float` | `postgleam.float` | `decode.float` |
| `numeric/decimal` | `String` | `postgleam.numeric` | `decode.numeric` |
| `text/varchar/char` | `String` | `postgleam.text` | `decode.text` |
| `bytea` | `BitArray` | `postgleam.bytea` | `decode.bytea` |
| `uuid` | `BitArray` | `postgleam.uuid` | `decode.uuid` |
| `json/jsonb` | `String` | `postgleam.json`/`.jsonb` | `decode.json`/`.jsonb` |
| `date` | `Int` | `postgleam.date` | `decode.date` |
| `timestamp` | `Int` | `postgleam.timestamp` | `decode.timestamp` |
| `timestamptz` | `Int` | `postgleam.timestamptz` | `decode.timestamptz` |
| nullable `X` | `Option(X)` | `postgleam.nullable` | `decode.optional` |

### SQLite

| SQLite | Gleam | Param | Decoder |
|--------|-------|-------|---------|
| `integer` | `Int` | `glite.int` | `decode.int` |
| `real` | `Float` | `glite.float` | `decode.float` |
| `text` | `String` | `glite.text` | `decode.text` |
| `blob` | `BitArray` | `glite.blob` | `decode.blob` |
| `boolean` | `Bool` | `glite.bool` | `decode.bool` |
| nullable `X` | `Option(X)` | `glite.nullable` | `decode.optional` |

## Building from Source

Requires Rust with the `wasm32-wasip1` target:

```sh
rustup target add wasm32-wasip1
make plugin
```

The WASM binary is output to `dist/sqlc-gen-gleam.wasm`.

## License

MIT
