// End-to-end tests for sqlc-gen-gleam generated SQLite code.
// These tests run against an in-memory SQLite database via glite.

import generated/query
import generated/tx/query as tx_query
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleeunit
import gleeunit/should
import glite
import glite/config

pub fn main() {
  gleeunit.main()
}

fn connect() {
  config.memory()
  |> config.foreign_keys(True)
  |> glite.connect()
  |> should.be_ok()
}

fn setup(conn) {
  // Create tables
  glite.exec(
    conn,
    "CREATE TABLE IF NOT EXISTS authors (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, bio TEXT)",
    [],
  )
  |> should.be_ok()
  glite.exec(
    conn,
    "CREATE TABLE IF NOT EXISTS books (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, author_id INTEGER NOT NULL, description TEXT, FOREIGN KEY (author_id) REFERENCES authors (id) ON DELETE CASCADE)",
    [],
  )
  |> should.be_ok()
  glite.exec(
    conn,
    "CREATE TABLE IF NOT EXISTS types_sqlite (c_integer INTEGER, c_real REAL, c_text TEXT, c_blob BLOB)",
    [],
  )
  |> should.be_ok()
  glite.exec(
    conn,
    "CREATE TABLE IF NOT EXISTS types_sqlite_affinity (id INTEGER PRIMARY KEY, c_numeric NUMERIC NOT NULL DEFAULT 0, c_decimal DECIMAL(10, 2), c_boolean BOOLEAN NOT NULL DEFAULT 0, c_date DATE, c_datetime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, c_timestamp TIMESTAMP, c_varchar VARCHAR(20), c_bigint BIGINT, c_double DOUBLE PRECISION, c_untyped)",
    [],
  )
  |> should.be_ok()
}

fn cleanup(conn) {
  glite.exec(conn, "DELETE FROM books", []) |> should.be_ok()
  glite.exec(conn, "DELETE FROM authors", []) |> should.be_ok()
}

// ============================================================
// Core query annotation tests
// ============================================================

// --- :one ---

pub fn test_one_test() {
  let conn = connect()
  setup(conn)

  let params =
    query.CreateAuthorParams(
      id: 1111,
      name: "Bojack Horseman",
      bio: Some("Back in the 90s"),
    )
  query.create_author(conn, params) |> should.be_ok()

  let author = query.get_author(conn, "Bojack Horseman") |> should.be_ok()
  should.equal(author.name, "Bojack Horseman")
  should.equal(author.bio, Some("Back in the 90s"))

  cleanup(conn)
  glite.disconnect(conn)
}

// --- :many ---

pub fn test_many_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(id: 1111, name: "Bojack Horseman", bio: None),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(
      id: 2222,
      name: "Dr. Seuss",
      bio: Some("Keep your eyes open"),
    ),
  )
  |> should.be_ok()

  let result =
    query.list_authors(conn, query.ListAuthorsParams(offset: 0, limit: 10))
    |> should.be_ok()
  should.equal(list.length(result), 2)

  // Check ordering (by name): Bojack < Dr. Seuss
  let first = list.first(result) |> should.be_ok()
  should.equal(first.name, "Bojack Horseman")

  cleanup(conn)
  glite.disconnect(conn)
}

// --- :exec ---

pub fn test_exec_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(id: 1111, name: "Bojack Horseman", bio: None),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 2222, name: "Dr. Seuss", bio: None),
  )
  |> should.be_ok()

  query.delete_author(conn, "Bojack Horseman") |> should.be_ok()

  // Should not find the deleted author
  let result = query.get_author(conn, "Bojack Horseman")
  should.be_error(result)

  // Other author should still exist
  let remaining = query.get_author(conn, "Dr. Seuss") |> should.be_ok()
  should.equal(remaining.name, "Dr. Seuss")

  cleanup(conn)
  glite.disconnect(conn)
}

// --- :execrows ---

pub fn test_exec_rows_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(id: 1, name: "A", bio: Some("Bio1")),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 2, name: "B", bio: Some("Bio2")),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 3, name: "C", bio: None),
  )
  |> should.be_ok()

  // Only 2 authors have bio IS NOT NULL
  let count = query.update_authors(conn, Some("Updated")) |> should.be_ok()
  should.equal(count, 2)

  cleanup(conn)
  glite.disconnect(conn)
}

// --- :execlastid ---

pub fn test_exec_last_id_test() {
  let conn = connect()
  setup(conn)

  let result =
    query.create_author_return_id(
      conn,
      query.CreateAuthorReturnIdParams(name: "NewAuthor", bio: Some("A bio")),
    )
    |> should.be_ok()
  should.be_true(result.id > 0)

  // Verify we can fetch the author by the returned id
  let author = query.get_author_by_id(conn, result.id) |> should.be_ok()
  should.equal(author.name, "NewAuthor")
  should.equal(author.bio, Some("A bio"))

  cleanup(conn)
  glite.disconnect(conn)
}

// ============================================================
// sqlc.narg() tests
// ============================================================

pub fn test_narg_null_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(id: 1, name: "Found", bio: None),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 2, name: "AlsoFound", bio: None),
  )
  |> should.be_ok()

  // With None pattern, COALESCE defaults to '%' — matches all
  let result = query.get_author_by_name_pattern(conn, None) |> should.be_ok()
  should.equal(list.length(result), 2)

  cleanup(conn)
  glite.disconnect(conn)
}

pub fn test_narg_not_null_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(id: 1, name: "Found", bio: None),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 2, name: "NotFound", bio: None),
  )
  |> should.be_ok()

  let result =
    query.get_author_by_name_pattern(conn, Some("Found")) |> should.be_ok()
  should.equal(list.length(result), 1)

  cleanup(conn)
  glite.disconnect(conn)
}

// ============================================================
// Join & embed tests
// ============================================================

pub fn test_join_embed_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(
      id: 1111,
      name: "Bojack Horseman",
      bio: Some("Back in the 90s"),
    ),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(
      id: 2222,
      name: "Dr. Seuss",
      bio: Some("Keep your eyes open"),
    ),
  )
  |> should.be_ok()

  query.create_book(
    conn,
    query.CreateBookParams(name: "One Trick Pony", author_id: 1111),
  )
  |> should.be_ok()
  query.create_book(
    conn,
    query.CreateBookParams(
      name: "How the Grinch Stole Christmas!",
      author_id: 2222,
    ),
  )
  |> should.be_ok()

  let result = query.list_all_authors_books(conn) |> should.be_ok()
  should.equal(list.length(result), 2)

  // Ordered by author name: Bojack < Dr. Seuss
  let first = list.first(result) |> should.be_ok()
  should.equal(first.authors_name, "Bojack Horseman")
  should.equal(first.books_name, "One Trick Pony")
  should.equal(first.books_author_id, 1111)

  cleanup(conn)
  glite.disconnect(conn)
}

pub fn test_self_join_embed_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(
      id: 1,
      name: "Albert Einstein",
      bio: Some("Physicist"),
    ),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(
      id: 2,
      name: "Albert Einstein",
      bio: Some("Also a physicist"),
    ),
  )
  |> should.be_ok()

  let result = query.get_duplicate_authors(conn) |> should.be_ok()
  should.equal(list.length(result), 1)

  let row = list.first(result) |> should.be_ok()
  should.equal(row.authors_name, "Albert Einstein")
  should.equal(row.authors_name_2, "Albert Einstein")
  should.be_true(row.authors_id < row.authors_id_2)

  cleanup(conn)
  glite.disconnect(conn)
}

// ============================================================
// Transaction tests
// ============================================================

pub fn test_transaction_test() {
  let conn = connect()
  setup(conn)

  // Statements inside a transaction use the queries generated for glite/tx.
  glite.transaction(conn, glite.Immediate, fn(tx) {
    tx_query.create_author(
      tx,
      tx_query.CreateAuthorParams(id: 1, name: "TxnAuthor", bio: None),
    )
  })
  |> should.be_ok()

  let author = query.get_author(conn, "TxnAuthor") |> should.be_ok()
  should.equal(author.name, "TxnAuthor")

  cleanup(conn)
  glite.disconnect(conn)
}

pub fn test_transaction_rollback_test() {
  let conn = connect()
  setup(conn)

  glite.transaction(conn, glite.Immediate, fn(tx) {
    tx_query.create_author(
      tx,
      tx_query.CreateAuthorParams(id: 1, name: "RollbackAuthor", bio: None),
    )
    |> should.be_ok()
    Error("rollback")
  })
  |> should.equal(Error(glite.RolledBack("rollback")))

  // Author should NOT exist after rollback
  let result = query.get_author(conn, "RollbackAuthor")
  should.be_error(result)

  cleanup(conn)
  glite.disconnect(conn)
}

// ============================================================
// SQLite types
// ============================================================

pub fn test_sqlite_types_test() {
  let conn = connect()
  setup(conn)
  query.delete_all_sqlite_types(conn) |> should.be_ok()

  query.insert_sqlite_types(
    conn,
    query.InsertSqliteTypesParams(
      c_integer: Some(42),
      c_real: Some(3.14),
      c_text: Some("hello world"),
      c_blob: Some(<<0x45, 0x42>>),
    ),
  )
  |> should.be_ok()

  let row = query.get_sqlite_types(conn) |> should.be_ok()
  should.equal(row.c_integer, Some(42))
  should.be_true(is_some_float_near(row.c_real, 3.14, 0.001))
  should.equal(row.c_text, Some("hello world"))
  should.equal(row.c_blob, Some(<<0x45, 0x42>>))

  query.delete_all_sqlite_types(conn) |> should.be_ok()
  glite.disconnect(conn)
}

pub fn test_sqlite_types_null_test() {
  let conn = connect()
  setup(conn)
  query.delete_all_sqlite_types(conn) |> should.be_ok()

  query.insert_sqlite_types(
    conn,
    query.InsertSqliteTypesParams(
      c_integer: None,
      c_real: None,
      c_text: None,
      c_blob: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_sqlite_types(conn) |> should.be_ok()
  should.equal(row.c_integer, None)
  should.equal(row.c_real, None)
  should.equal(row.c_text, None)
  should.equal(row.c_blob, None)

  query.delete_all_sqlite_types(conn) |> should.be_ok()
  glite.disconnect(conn)
}

pub fn test_sqlite_types_count_test() {
  let conn = connect()
  setup(conn)
  query.delete_all_sqlite_types(conn) |> should.be_ok()

  query.insert_sqlite_types(
    conn,
    query.InsertSqliteTypesParams(
      c_integer: Some(1),
      c_real: Some(1.0),
      c_text: Some("a"),
      c_blob: None,
    ),
  )
  |> should.be_ok()
  query.insert_sqlite_types(
    conn,
    query.InsertSqliteTypesParams(
      c_integer: Some(1),
      c_real: Some(1.0),
      c_text: Some("a"),
      c_blob: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_sqlite_types_cnt(conn) |> should.be_ok()
  should.equal(row.cnt, 2)
  should.equal(row.c_integer, Some(1))

  query.delete_all_sqlite_types(conn) |> should.be_ok()
  glite.disconnect(conn)
}

// ============================================================
// Slice parameter tests
// ============================================================

pub fn test_slice_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(id: 1, name: "Author1", bio: None),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 2, name: "Author2", bio: None),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 3, name: "Author3", bio: None),
  )
  |> should.be_ok()

  let result = query.get_authors_by_ids(conn, [1, 3]) |> should.be_ok()
  should.equal(list.length(result), 2)

  let first = list.first(result) |> should.be_ok()
  should.be_true(first.id == 1 || first.id == 3)

  cleanup(conn)
  glite.disconnect(conn)
}

pub fn test_multiple_slices_test() {
  let conn = connect()
  setup(conn)

  query.create_author(
    conn,
    query.CreateAuthorParams(id: 1, name: "Alpha", bio: None),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 2, name: "Beta", bio: None),
  )
  |> should.be_ok()
  query.create_author(
    conn,
    query.CreateAuthorParams(id: 3, name: "Gamma", bio: None),
  )
  |> should.be_ok()

  let result =
    query.get_authors_by_ids_and_names(
      conn,
      query.GetAuthorsByIdsAndNamesParams(ids: [1, 2, 3], names: [
        "Alpha",
        "Gamma",
      ]),
    )
    |> should.be_ok()
  should.equal(list.length(result), 2)

  cleanup(conn)
  glite.disconnect(conn)
}

// ============================================================
// :copyfrom batch insert test
// ============================================================

pub fn test_copy_from_test() {
  let conn = connect()
  setup(conn)
  query.delete_all_sqlite_types(conn) |> should.be_ok()

  query.insert_sqlite_types_batch(conn, [
    query.InsertSqliteTypesBatchParams(
      c_integer: Some(1),
      c_real: Some(1.5),
      c_text: Some("hello"),
      c_blob: Some(<<0x01, 0x02>>),
    ),
    query.InsertSqliteTypesBatchParams(
      c_integer: Some(2),
      c_real: Some(2.5),
      c_text: Some("world"),
      c_blob: Some(<<0x03, 0x04>>),
    ),
    query.InsertSqliteTypesBatchParams(
      c_integer: Some(1),
      c_real: Some(1.5),
      c_text: Some("hello"),
      c_blob: Some(<<0x01, 0x02>>),
    ),
  ])
  |> should.be_ok()

  let row = query.get_sqlite_types_cnt(conn) |> should.be_ok()
  should.equal(row.cnt, 2)
  should.equal(row.c_integer, Some(1))

  query.delete_all_sqlite_types(conn) |> should.be_ok()
  glite.disconnect(conn)
}

// ============================================================
// Type override test
// ============================================================

pub fn test_type_override_test() {
  let conn = connect()
  setup(conn)
  query.delete_all_sqlite_types(conn) |> should.be_ok()

  query.insert_sqlite_types(
    conn,
    query.InsertSqliteTypesParams(
      c_integer: Some(42),
      c_real: Some(3.14),
      c_text: Some("override test"),
      c_blob: None,
    ),
  )
  |> should.be_ok()

  // GetSqliteFunctions columns are overridden:
  //   max_integer: Option(Int) (override from default String)
  //   max_real: Option(Float) (override from default String)
  //   max_text: String (override to non-nullable)
  let result = query.get_sqlite_functions(conn) |> should.be_ok()
  should.equal(result.max_integer, Some(42))
  should.be_true(is_some_float_near(result.max_real, 3.14, 0.001))
  // max_text is String (non-nullable via override), not Option(String)
  should.equal(result.max_text, "override test")

  query.delete_all_sqlite_types(conn) |> should.be_ok()
  glite.disconnect(conn)
}

// ============================================================
// Declared types with NUMERIC affinity
// ============================================================

fn affinity(
  numeric: Float,
  bigint: Int,
  label: String,
) -> query.InsertSqliteAffinityParams {
  query.InsertSqliteAffinityParams(
    c_numeric: numeric,
    c_decimal: Some(3.0),
    c_boolean: True,
    c_date: Some("2026-01-02"),
    c_timestamp: Some("1700000000"),
    c_varchar: Some(label),
    c_bigint: Some(bigint),
    c_double: Some(2.0),
    c_untyped: Some("42"),
  )
}

// NUMERIC and DECIMAL columns are floats, read back whether SQLite stored
// an INTEGER or a REAL. Date and time columns are strings, read back even
// when the text looked like a number and was stored as one.
pub fn numeric_affinity_round_trip_test() {
  let conn = connect()
  setup(conn)

  let row =
    query.insert_sqlite_affinity(conn, affinity(12.5, 9_000_000_000, "x"))
    |> should.be_ok()
  should.equal(row.c_numeric, 12.5)
  // 3.0 is stored as the integer 3.
  should.equal(row.c_decimal, Some(3.0))
  should.equal(row.c_boolean, True)
  should.equal(row.c_date, Some("2026-01-02"))
  // "1700000000" is stored as an integer.
  should.equal(row.c_timestamp, Some("1700000000"))
  should.equal(row.c_varchar, Some("x"))
  should.equal(row.c_bigint, Some(9_000_000_000))
  should.equal(row.c_double, Some(2.0))
  should.equal(row.c_untyped, Some("42"))
  // The CURRENT_TIMESTAMP default: "YYYY-MM-DD HH:MM:SS".
  should.equal(string.length(row.c_datetime), 19)

  let whole =
    query.insert_sqlite_affinity(
      conn,
      query.InsertSqliteAffinityParams(
        c_numeric: 2.0,
        c_decimal: None,
        c_boolean: False,
        c_date: None,
        c_timestamp: None,
        c_varchar: None,
        c_bigint: None,
        c_double: None,
        c_untyped: None,
      ),
    )
    |> should.be_ok()
  should.equal(whole.c_numeric, 2.0)
  should.equal(whole.c_decimal, None)

  let read = query.get_sqlite_affinity(conn, row.id) |> should.be_ok()
  should.equal(read.c_numeric, 12.5)
  should.equal(read.c_decimal, Some(3.0))
  should.equal(read.c_timestamp, Some("1700000000"))

  // The same queries inside a transaction.
  let in_tx =
    glite.transaction(conn, glite.Immediate, fn(tx) {
      tx_query.insert_sqlite_affinity(
        tx,
        tx_query.InsertSqliteAffinityParams(
          c_numeric: 7.0,
          c_decimal: Some(0.25),
          c_boolean: True,
          c_date: None,
          c_timestamp: None,
          c_varchar: None,
          c_bigint: None,
          c_double: None,
          c_untyped: None,
        ),
      )
    })
    |> should.be_ok()
  should.equal(in_tx.c_numeric, 7.0)
  should.equal(in_tx.c_decimal, Some(0.25))

  glite.disconnect(conn)
}

// sum() of whole numbers is an INTEGER and max() of a DATETIME column has
// no declared type; both are read without an override.
pub fn sqlite_aggregates_test() {
  let conn = connect()
  setup(conn)

  let empty = query.get_sqlite_affinity_aggregates(conn) |> should.be_ok()
  should.equal(empty.cnt, 0)
  should.equal(empty.sum_numeric, None)
  should.equal(empty.max_datetime, None)
  should.equal(empty.total_bigint, 0.0)

  query.insert_sqlite_affinity(conn, affinity(2.0, 10, "a")) |> should.be_ok()
  query.insert_sqlite_affinity(conn, affinity(3.0, 20, "b")) |> should.be_ok()

  let stats = query.get_sqlite_affinity_aggregates(conn) |> should.be_ok()
  should.equal(stats.cnt, 2)
  should.equal(stats.sum_numeric, Some(5.0))
  should.equal(stats.avg_bigint, Some(15.0))
  should.equal(stats.total_bigint, 30.0)
  let assert Some(latest) = stats.max_datetime
  should.equal(string.length(latest), 19)

  glite.disconnect(conn)
}

// sqlc.arg() and sqlc.slice() in one query: the parameters keep the names
// from the SQL, and the slice may hold any number of items, even though
// sqlc numbers the other parameters (?1, ?3, ?4) around it.
pub fn named_params_with_slice_test() {
  let conn = connect()
  setup(conn)
  query.insert_sqlite_affinity(conn, affinity(1.0, 1, "a")) |> should.be_ok()
  query.insert_sqlite_affinity(conn, affinity(2.0, 2, "b")) |> should.be_ok()
  query.insert_sqlite_affinity(conn, affinity(3.0, 3, "a")) |> should.be_ok()

  let find = fn(ids, min_numeric, label) {
    query.get_sqlite_affinity_by_ids(
      conn,
      query.GetSqliteAffinityByIdsParams(
        flag: True,
        ids: ids,
        min_numeric: min_numeric,
        label: Some(label),
      ),
    )
    |> should.be_ok()
    |> list.map(fn(r) { r.id })
  }
  should.equal(find([1, 2, 3], 0.0, "a"), [1, 3])
  should.equal(find([1, 2, 3], 2.5, "a"), [3])
  should.equal(find([1, 2], 0.0, "b"), [2])
  should.equal(find([3], 0.0, "a"), [3])
  should.equal(find([], 0.0, "a"), [])
  // The text inside the SQL string literal is not a parameter.
  should.equal(find([1, 2, 3], 0.0, "?1 -- not a parameter"), [1, 2, 3])

  glite.disconnect(conn)
}

// ============================================================
// Helpers
// ============================================================

fn is_some_float_near(
  val: option.Option(Float),
  expected: Float,
  tolerance: Float,
) -> Bool {
  case val {
    Some(v) -> float_abs(v -. expected) <. tolerance
    None -> False
  }
}

fn float_abs(x: Float) -> Float {
  case x <. 0.0 {
    True -> 0.0 -. x
    False -> x
  }
}
