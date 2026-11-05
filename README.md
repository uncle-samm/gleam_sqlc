# sqlc-gen-gleam

A [sqlc](https://sqlc.dev) WASM plugin that generates type-safe [Gleam](https://gleam.run) code from SQL.

- **PostgreSQL** via [postgleam](https://hex.pm/packages/postgleam) (0.9 or later)
- **SQLite** via [glite](https://hex.pm/packages/glite) (0.4 or later)

You write SQL, sqlc checks it against your schema, and this plugin generates Gleam
row types, parameter types and query functions that return the driver's own
`Result` types.

> **A personal project.** I built this mainly for my own use and share it in case it helps
> you too. There are no promises about support, response times or a roadmap. Issues and
> pull requests are welcome, and so is forking it to make it your own.

## Quick start

### 1. Configure sqlc

Download the plugin from the [latest release](https://github.com/uncle-samm/gleam_sqlc/releases/latest).
Each release has `sqlc-gen-gleam.wasm` and a `sqlc-gen-gleam.wasm.sha256` file with its checksum.

```yaml
# sqlc.yaml
version: "2"
plugins:
  - name: gleam
    wasm:
      url: https://github.com/uncle-samm/gleam_sqlc/releases/download/v0.9.0/sqlc-gen-gleam.wasm
      sha256: "<contents of sqlc-gen-gleam.wasm.sha256 for that release>"
sql:
  - schema: "schema.sql"
    queries: "query.sql"
    engine: "postgresql" # or "sqlite"
    codegen:
      - plugin: gleam
        out: "src/db"
```

Put `out` inside your project's `src/` directory. The plugin uses the path to
work out the Gleam module name of the generated models (here `db/models`).

### 2. Write SQL

```sql
-- schema.sql
CREATE TYPE author_status AS ENUM ('active', 'retired');

CREATE TABLE authors (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    bio TEXT,
    status author_status NOT NULL DEFAULT 'active'
);

-- query.sql

-- name: GetAuthor :one
SELECT * FROM authors WHERE name = $1 LIMIT 1;

-- name: ListAuthors :many
SELECT * FROM authors ORDER BY name;

-- name: CreateAuthor :one
INSERT INTO authors (name, bio, status) VALUES ($1, $2, $3) RETURNING *;

-- name: DeleteAuthor :exec
DELETE FROM authors WHERE name = $1;

-- name: UpdateBios :execrows
UPDATE authors SET bio = $1 WHERE bio IS NOT NULL;

-- name: InsertAuthors :copyfrom
INSERT INTO authors (name, bio) VALUES ($1, $2);
```

### 3. Generate

```sh
sqlc generate
```

This writes `src/db/models.gleam` (table and enum types) and one module per
queries file (`src/db/query.gleam`).

### 4. Use the generated code

```gleam
import db/models
import db/query
import gleam/option.{None, Some}
import postgleam
import postgleam/config

pub fn main() {
  let assert Ok(conn) =
    config.default()
    |> config.database("mydb")
    |> postgleam.connect()

  // :one returns Result(Row, postgleam/error.Error)
  let assert Ok(author) =
    query.create_author(
      conn,
      query.CreateAuthorParams(name: "Alice", bio: None, status: models.Active),
    )

  // :many returns Result(postgleam.Response(Row), Error)
  let assert Ok(response) = query.list_authors(conn)
  let _authors = response.rows

  // :exec returns Result(Nil, Error)
  let assert Ok(Nil) = query.delete_author(conn, "Bob")

  // :execrows returns Result(Int, Error), the number of rows changed
  let assert Ok(_count) = query.update_bios(conn, Some("Updated bio"))

  // :copyfrom inserts every row in one pipelined batch, all or nothing
  let assert Ok(2) =
    query.insert_authors(conn, [
      query.InsertAuthorsParams(name: "Carol", bio: None),
      query.InsertAuthorsParams(name: "Dan", bio: Some("Writes")),
    ])

  author.id
}
```

## Generated code

For the SQL above the plugin generates (abridged):

**`models.gleam`**

```gleam
import gleam/option.{type Option}

pub type AuthorStatus {
  Active
  Retired
}

pub fn author_status_to_string(value: AuthorStatus) -> String { ... }
pub fn author_status_from_string(value: String) -> Result(AuthorStatus, Nil) { ... }

pub type Author {
  Author(id: Int, name: String, bio: Option(String), status: AuthorStatus)
}
```

**`query.gleam`**

```gleam
const get_author_sql = "SELECT id, name, bio, status FROM authors WHERE name = $1 LIMIT 1"

pub type GetAuthorRow {
  GetAuthorRow(id: Int, name: String, bio: Option(String), status: models.AuthorStatus)
}

pub fn get_author(conn, name: String) {
  let decoder = {
    use c0 <- decode.element(0, decode.int)
    use c1 <- decode.element(1, decode.text)
    use c2 <- decode.element(2, decode.optional(decode.text))
    use c3 <- decode.element(3, decode_author_status)
    decode.success(GetAuthorRow(id: c0, name: c1, bio: c2, status: c3))
  }
  postgleam.query_one(conn, get_author_sql, [Some(value.Text(name))], decoder)
}
```

Imports are derived from the generated code, so the modules compile with no
unused-import warnings. SQL comments above a query become `///` doc comments on
its function.

## Query annotations

| Annotation | PostgreSQL return type | SQLite return type |
|------------|------------------------|--------------------|
| `:one` | `Result(Row, Error)` | `Result(Row, Error)` |
| `:many` | `Result(postgleam.Response(Row), Error)` | `Result(List(Row), Error)` |
| `:exec` | `Result(Nil, Error)` | `Result(Nil, Error)` |
| `:execrows` | `Result(Int, Error)` | `Result(Int, Error)` |
| `:execlastid` | as `:one` (with `RETURNING`) or `:exec` | as `:one` or `:exec` |
| `:copyfrom` | `Result(Int, Error)`, one atomic `query_batch` | `Result(Int, Error)`, one atomic `exec_many` |

`Error` is always the driver's own error type, so you can match on database
errors (for example a unique violation). `:one` with no matching row returns
`DecodeError` for PostgreSQL and `Decode` for SQLite. With glite, `:one` also
fails when the query returns more than one row, so add `LIMIT 1` where needed.

Other sqlc commands (`:batchexec`, `:batchmany`, `:batchone`, `:execresult`)
are rejected with an error at generation time.

## Features

- **Type mapping** for PostgreSQL and SQLite types (tables below)
- **Nullable columns** become `Option(T)`, including nullability lost through casts
  when the source column can be identified unambiguously
- **Parameter ergonomics**: no params means no argument, one param is a named
  argument, two or more become a `<Query>Params` record
- **Enums**: PostgreSQL enums become Gleam custom types with `to_string` /
  `from_string` helpers, and enum columns and params use those types
- **Arrays**: PostgreSQL array columns and params map to `List(T)`
- **`sqlc.arg('name')` / `sqlc.narg('name')`** for parameter names and forced nullability
- **`sqlc.embed(table)`** expands embedded tables into prefixed fields
- **`sqlc.slice('name')`** for dynamic `IN (...)` lists (SQLite)
- **Type overrides** per column
- **Safe names**: SQL names that are not valid Gleam (spaces, leading digits,
  keywords, clashing enum values) are rewritten, and strings and comments are escaped

## Plugin options

```yaml
codegen:
  - plugin: gleam
    out: "src/db"
    options:
      module: "postgleam/pool"
      uuidAsString: true
      overrides:
        - column: "GetAuthor:bio"
          gleamType:
            type: "String"
            notNull: true
```

Unknown options, wrong value types and invalid values fail generation with an
error, so a typo cannot silently change the output.

| Option | Default | Description |
|--------|---------|-------------|
| `module` | `postgleam` / `glite` | Module whose query functions are called. The last path segment is the alias used in code. |
| `decodeModule` | `{module}/decode` | Decode module import. Its last segment must be `decode`. |
| `uuidAsString` | `false` | Map `uuid` to `String` instead of `BitArray`. Params are parsed before the query runs; an invalid UUID returns `EncodeError`. PostgreSQL only. |
| `modelsModule` | derived from `out` | Gleam module path of `models.gleam`, needed for enum types when `out` is not under `src/`. |
| `skipModels` | `false` | Don't generate `models.gleam`. Enum columns are then typed as `String`. |
| `overrides` | `[]` | Override a column's type: `QueryName:column` or `*:column`, with `type` one of `String`, `Int`, `Float`, `Bool`, `BitArray`. |

`postgleamVersion` and `queryTimeout` are deprecated: they are still accepted, print a
warning and have no effect. With postgleam 0.9, set timeouts on the connection or pool config.

### Targets

| Target | Options | `conn` is |
|--------|---------|-----------|
| postgleam connection (default) | none | `postgleam.Connection` |
| postgleam pool | `module: "postgleam/pool"` | `pool.Pool` (timeouts come from the pool config) |
| glite connection (default) | none | `glite.Connection` |
| glite transaction | `module: "glite/tx"`, `skipModels: true` | the `Tx` passed to `glite.transaction` |
| Your own wrapper | `module: "app/db"` | whatever your wrapper takes; it needs `query`, `query_with`, `query_one` and `query_batch` with postgleam's signatures |

To use the same queries both inside and outside glite transactions, add a second
codegen entry with `module: "glite/tx"`, `skipModels: true` and a different `out`
(see [`end2end/sqlc.yaml`](end2end/sqlc.yaml)).

A wrapper module must provide the same functions as the driver it replaces
(`query_one`, `query_with`, `query`, `query_batch` for PostgreSQL;
`query_one`, `query`, `exec`, `exec_many` for SQLite) and use the driver's
value and decode modules.

## Type mapping

### PostgreSQL

| PostgreSQL | Gleam | Param | Decoder |
|------------|-------|-------|---------|
| `bool` | `Bool` | `value.Boolean` | `decode.bool` |
| `int2` / `int4` / `int8` / `serial` / `bigserial` | `Int` | `value.Integer` | `decode.int` |
| `float4` / `float8` | `Float` | `value.Float` | `decode.float` |
| `numeric` / `decimal` | `String` | `value.Numeric` | `decode.numeric` |
| `money` | `Int` (cents) | `value.Money` | `decode.money` |
| `text` / `varchar` / `char` / `bpchar` / `name` | `String` | `value.Text` | `decode.text` |
| `bytea` | `BitArray` | `value.Bytea` | `decode.bytea` |
| `uuid` | `BitArray` (or `String` with `uuidAsString`) | `value.Uuid` | `decode.uuid` / `decode.uuid_string` |
| `json` / `jsonb` / `jsonpath` / `xml` | `String` | `value.Json` / `Jsonb` / `Jsonpath` / `Xml` | `decode.json` / ... |
| `date` | `Int` (days) | `value.Date` | `decode.date` |
| `time` | `Int` (µs) | `value.Time` | `decode.time` |
| `timetz` | `#(Int, Int)` | `value.TimeTz` | `decode.timetz` |
| `timestamp` / `timestamptz` | `Int` (µs) | `value.Timestamp` / `Timestamptz` | `decode.timestamp` / `timestamptz` |
| `interval` | `#(Int, Int, Int)` | `value.Interval` | `decode.interval` |
| `point` / `line` / `lseg` / `box` / `circle` / `path` / `polygon` | tuples of `Float` | `value.Point` / ... | `decode.point` / ... |
| `inet` / `cidr` | `#(Int, BitArray, Int)` | `value.Inet` | `decode.inet` |
| `macaddr` / `macaddr8` | `BitArray` | `value.Macaddr` / `Macaddr8` | `decode.macaddr` / `macaddr8` |
| `bit` / `varbit` | `#(Int, BitArray)` | `value.BitString` | `decode.bit_string` |
| enum types | the generated custom type | its text value | its `from_string` |
| `T[]` | `List(T)` | `value.Array` | array of the element decoder |
| nullable `T` | `Option(T)` | `None` → `NULL` | `decode.optional` |

Other types (for example `tsvector` or types from extensions) fall back to
`String` with a warning; use an override if you need something else.

### SQLite

| SQLite | Gleam | Param | Decoder |
|--------|-------|-------|---------|
| `INTEGER` (and other `INT` types) | `Int` | `glite.int` | `decode.int` |
| `REAL` / `DOUBLE` / `FLOAT` | `Float` | `glite.float` | `decode.float` |
| `TEXT` / `VARCHAR` / `CHAR` / `CLOB` | `String` | `glite.text` | `decode.text` |
| `BLOB` | `BitArray` | `glite.blob` | `decode.blob` |
| `BOOLEAN` | `Bool` | `glite.bool` | `decode.bool` |
| `NUMERIC` / `DECIMAL` / `DATE` / `DATETIME` / `TIMESTAMP` | `String` | `glite.text` | `decode.text` |
| nullable `T` | `Option(T)` | `glite.nullable` | `decode.optional` |

## Development

Requires Rust with the `wasm32-wasip1` target, `protoc`, sqlc 1.30, and Gleam 1.19
with Erlang/OTP 28 for the end-to-end tests. [`.mise.toml`](.mise.toml) pins them all.

```sh
rustup target add wasm32-wasip1
make lint           # cargo fmt --check and clippy
make test           # Rust unit tests
make plugin         # builds dist/sqlc-gen-gleam.wasm and updates the sha256 in the example configs
make generate       # regenerates examples/ and end2end/
make e2e            # PostgreSQL (via Docker) and SQLite end-to-end tests
```

CI regenerates every example, fails if the committed Gleam code is out of date,
compiles the generated code with `--warnings-as-errors`, and runs both
end-to-end suites.

## License

MIT
