// End-to-end tests for sqlc-gen-gleam generated SQLite code.
// These tests run against an in-memory SQLite database via glite.

import gleam/list
import gleam/option.{None, Some}
import gleeunit
import gleeunit/should
import glite
import glite/config
import glite/error
import generated/query

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
  should.equal(result.count, 2)

  // Check ordering (by name): Bojack < Dr. Seuss
  let first = list.first(result.rows) |> should.be_ok()
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
  let result =
    query.get_author_by_name_pattern(conn, None) |> should.be_ok()
  should.equal(result.count, 2)

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
  should.equal(result.count, 1)

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
  should.equal(result.count, 2)

  // Ordered by author name: Bojack < Dr. Seuss
  let first = list.first(result.rows) |> should.be_ok()
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
  should.equal(result.count, 1)

  let row = list.first(result.rows) |> should.be_ok()
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

  glite.transaction(conn, fn(conn) {
    query.create_author(
      conn,
      query.CreateAuthorParams(id: 1, name: "TxnAuthor", bio: None),
    )
    |> should.be_ok()
    Ok(Nil)
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

  glite.transaction(conn, fn(conn) {
    query.create_author(
      conn,
      query.CreateAuthorParams(id: 1, name: "RollbackAuthor", bio: None),
    )
    |> should.be_ok()
    Error(error.ConnectionError("rollback"))
  })
  |> should.be_error()

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
  should.equal(result.count, 2)

  let first = list.first(result.rows) |> should.be_ok()
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
      query.GetAuthorsByIdsAndNamesParams(
        id: [1, 2, 3],
        name: ["Alpha", "Gamma"],
      ),
    )
    |> should.be_ok()
  should.equal(result.count, 2)

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
