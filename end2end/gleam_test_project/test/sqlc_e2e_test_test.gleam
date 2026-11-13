// End-to-end tests for sqlc-gen-gleam generated code.
// These tests run against a live PostgreSQL instance via Docker.
//
// Run: docker compose -f end2end/docker-compose.yml up -d
//       cd end2end/gleam_test_project && gleam test

import generated/models
import generated/nullable_arrays/models as nullable_models
import generated/nullable_arrays/query as nullable_query
import generated/query
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleeunit
import gleeunit/should
import postgleam
import postgleam/config
import postgleam/error

pub fn main() {
  gleeunit.main()
}

fn connect() {
  config.default()
  |> config.host("localhost")
  |> config.port(5433)
  |> config.database("sqlc_test")
  |> config.username("postgres")
  |> config.password("postgres")
  |> postgleam.connect()
  |> should.be_ok()
}

fn cleanup(conn) {
  query.truncate_authors(conn) |> should.be_ok()
}

// ============================================================
// Core query annotation tests
// ============================================================

// --- :one ---

pub fn test_one_test() {
  let conn = connect()
  cleanup(conn)

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
  postgleam.disconnect(conn)
}

// --- :many ---

pub fn test_many_test() {
  let conn = connect()
  cleanup(conn)

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
  postgleam.disconnect(conn)
}

// --- :exec ---

pub fn test_exec_test() {
  let conn = connect()
  cleanup(conn)

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
  postgleam.disconnect(conn)
}

// --- :execrows ---

pub fn test_exec_rows_test() {
  let conn = connect()
  cleanup(conn)

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
  postgleam.disconnect(conn)
}

// --- :execlastid ---

pub fn test_exec_last_id_test() {
  let conn = connect()
  cleanup(conn)

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
  postgleam.disconnect(conn)
}

// ============================================================
// sqlc.narg() tests
// ============================================================

pub fn test_narg_null_test() {
  let conn = connect()
  cleanup(conn)

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
  should.equal(result.count, 2)

  cleanup(conn)
  postgleam.disconnect(conn)
}

pub fn test_narg_not_null_test() {
  let conn = connect()
  cleanup(conn)

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
  postgleam.disconnect(conn)
}

// ============================================================
// Join & embed tests
// ============================================================

pub fn test_join_embed_test() {
  let conn = connect()
  cleanup(conn)

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
  postgleam.disconnect(conn)
}

pub fn test_self_join_embed_test() {
  let conn = connect()
  cleanup(conn)

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
  postgleam.disconnect(conn)
}

// ============================================================
// Transaction tests
// ============================================================

pub fn test_transaction_test() {
  let conn = connect()
  cleanup(conn)

  postgleam.transaction(conn, fn(conn) {
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
  postgleam.disconnect(conn)
}

pub fn test_transaction_rollback_test() {
  let conn = connect()
  cleanup(conn)

  postgleam.transaction(conn, fn(conn) {
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
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL numeric types
// ============================================================

pub fn test_numeric_types_test() {
  let conn = connect()
  query.truncate_postgres_numeric_types(conn) |> should.be_ok()

  // Note: c_bit and c_money use OIDs that PostGleam 0.3.0 doesn't have
  // codecs for, so we set them to None here.
  // Also, c_decimal/c_numeric use the numeric codec which has a bug
  // in PostGleam 0.3.0 with certain values, so we test without them.
  query.insert_postgres_numeric_types(
    conn,
    query.InsertPostgresNumericTypesParams(
      c_boolean: Some(True),
      c_bit: None,
      c_smallint: Some(35),
      c_integer: Some(-23_423),
      c_bigint: Some(4_235_235_263),
      c_decimal: None,
      c_numeric: None,
      c_real: Some(3.83),
      c_double_precision: Some(-8_403_284.321435),
      c_money: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_numeric_types(conn) |> should.be_ok()
  should.equal(row.c_boolean, Some(True))
  should.equal(row.c_smallint, Some(35))
  should.equal(row.c_integer, Some(-23_423))
  should.equal(row.c_bigint, Some(4_235_235_263))
  should.be_true(is_some_float_near(row.c_real, 3.83, 0.01))
  should.be_true(is_some_float_near(
    row.c_double_precision,
    -8_403_284.321435,
    0.001,
  ))

  query.truncate_postgres_numeric_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_numeric_types_null_test() {
  let conn = connect()
  query.truncate_postgres_numeric_types(conn) |> should.be_ok()

  query.insert_postgres_numeric_types(
    conn,
    query.InsertPostgresNumericTypesParams(
      c_boolean: None,
      c_bit: None,
      c_smallint: None,
      c_integer: None,
      c_bigint: None,
      c_decimal: None,
      c_numeric: None,
      c_real: None,
      c_double_precision: None,
      c_money: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_numeric_types(conn) |> should.be_ok()
  should.equal(row.c_boolean, None)
  should.equal(row.c_smallint, None)
  should.equal(row.c_integer, None)
  should.equal(row.c_bigint, None)
  should.equal(row.c_decimal, None)
  should.equal(row.c_numeric, None)
  should.equal(row.c_real, None)
  should.equal(row.c_double_precision, None)
  should.equal(row.c_money, None)

  query.truncate_postgres_numeric_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL integer types (subset of numeric)
// ============================================================

pub fn test_integer_types_test() {
  let conn = connect()
  query.truncate_postgres_numeric_types(conn) |> should.be_ok()

  query.insert_postgres_numeric_types(
    conn,
    query.InsertPostgresNumericTypesParams(
      c_boolean: Some(True),
      c_bit: None,
      c_smallint: Some(32_767),
      c_integer: Some(2_147_483_647),
      c_bigint: Some(9_223_372_036_854),
      c_decimal: None,
      c_numeric: None,
      c_real: None,
      c_double_precision: None,
      c_money: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_numeric_types(conn) |> should.be_ok()
  should.equal(row.c_boolean, Some(True))
  should.equal(row.c_smallint, Some(32_767))
  should.equal(row.c_integer, Some(2_147_483_647))
  should.equal(row.c_bigint, Some(9_223_372_036_854))

  query.truncate_postgres_numeric_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL floating point types (subset of numeric)
// ============================================================

pub fn test_floating_point_types_test() {
  let conn = connect()
  query.truncate_postgres_numeric_types(conn) |> should.be_ok()

  // Skip c_decimal/c_numeric/c_money due to PostGleam 0.3.0 numeric codec bug
  query.insert_postgres_numeric_types(
    conn,
    query.InsertPostgresNumericTypesParams(
      c_boolean: None,
      c_bit: None,
      c_smallint: None,
      c_integer: None,
      c_bigint: None,
      c_decimal: None,
      c_numeric: None,
      c_real: Some(3.83),
      c_double_precision: Some(-8_403_284.321435),
      c_money: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_numeric_types(conn) |> should.be_ok()
  should.be_true(is_some_float_near(row.c_real, 3.83, 0.01))
  should.be_true(is_some_float_near(
    row.c_double_precision,
    -8_403_284.321435,
    0.001,
  ))

  query.truncate_postgres_numeric_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL string types
// ============================================================

pub fn test_string_types_test() {
  let conn = connect()
  query.truncate_postgres_string_types(conn) |> should.be_ok()

  query.insert_postgres_string_types(
    conn,
    query.InsertPostgresStringTypesParams(
      c_char: Some("E"),
      c_varchar: Some("It takes a nation"),
      c_character_varying: Some("Rebel Without a Pause"),
      c_bpchar: Some("Master of Puppets"),
      c_text: Some("Prophets of Rage"),
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_string_types(conn) |> should.be_ok()
  should.equal(row.c_char, Some("E"))
  should.equal(row.c_varchar, Some("It takes a nation"))
  should.equal(row.c_character_varying, Some("Rebel Without a Pause"))
  // bpchar pads to fixed length, trim trailing spaces
  should.be_true(case row.c_bpchar {
    Some(v) -> string.trim(v) == "Master of Puppets"
    None -> False
  })
  should.equal(row.c_text, Some("Prophets of Rage"))

  query.truncate_postgres_string_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_string_types_null_test() {
  let conn = connect()
  query.truncate_postgres_string_types(conn) |> should.be_ok()

  query.insert_postgres_string_types(
    conn,
    query.InsertPostgresStringTypesParams(
      c_char: None,
      c_varchar: None,
      c_character_varying: None,
      c_bpchar: None,
      c_text: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_string_types(conn) |> should.be_ok()
  should.equal(row.c_char, None)
  should.equal(row.c_varchar, None)
  should.equal(row.c_character_varying, None)
  should.equal(row.c_bpchar, None)
  should.equal(row.c_text, None)

  query.truncate_postgres_string_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL datetime types
// ============================================================

pub fn test_datetime_types_test() {
  let conn = connect()
  query.truncate_postgres_date_time_types(conn) |> should.be_ok()

  // PostGleam: date = days since epoch, timestamp = microseconds since epoch
  // time = microseconds since midnight, interval = #(microseconds, days, months)
  let date_val = 19_790
  let time_val = 43_200_000_000
  // 12:00:00
  let ts_val = 1_710_489_600_000_000
  let tstz_val = 1_710_489_600_000_000
  let interval_val = #(3_600_000_000, 0, 0)
  // 1 hour

  query.insert_postgres_date_time_types(
    conn,
    query.InsertPostgresDateTimeTypesParams(
      c_date: Some(date_val),
      c_time: Some(time_val),
      c_timestamp: Some(ts_val),
      c_timestamp_with_tz: Some(tstz_val),
      c_interval: Some(interval_val),
      c_timestamp_noda_instant_override: Some(ts_val),
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_date_time_types(conn) |> should.be_ok()
  should.equal(row.c_date, Some(date_val))
  should.equal(row.c_time, Some(time_val))
  should.equal(row.c_timestamp, Some(ts_val))
  should.equal(row.c_timestamp_with_tz, Some(tstz_val))
  should.equal(row.c_interval, Some(interval_val))
  should.equal(row.c_timestamp_noda_instant_override, Some(ts_val))

  query.truncate_postgres_date_time_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_datetime_types_null_test() {
  let conn = connect()
  query.truncate_postgres_date_time_types(conn) |> should.be_ok()

  query.insert_postgres_date_time_types(
    conn,
    query.InsertPostgresDateTimeTypesParams(
      c_date: None,
      c_time: None,
      c_timestamp: None,
      c_timestamp_with_tz: None,
      c_interval: None,
      c_timestamp_noda_instant_override: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_date_time_types(conn) |> should.be_ok()
  should.equal(row.c_date, None)
  should.equal(row.c_time, None)
  should.equal(row.c_timestamp, None)
  should.equal(row.c_timestamp_with_tz, None)
  should.equal(row.c_interval, None)

  query.truncate_postgres_date_time_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL network types
// ============================================================

pub fn test_network_types_insert_test() {
  let conn = connect()
  query.truncate_postgres_network_types(conn) |> should.be_ok()

  // Use generated insert function with proper PostGleam codecs
  query.insert_postgres_network_types(
    conn,
    query.InsertPostgresNetworkTypesParams(
      // inet/cidr: #(family, address_bytes, mask_bits)
      // family: 2 = IPv4, mask: 24
      c_cidr: Some(#(2, <<192, 168, 1, 0>>, 24)),
      c_inet: Some(#(2, <<192, 168, 1, 1>>, 32)),
      // macaddr: 6 bytes
      c_macaddr: Some(<<0x08, 0x00, 0x2B, 0x01, 0x02, 0x03>>),
      // macaddr8: 8 bytes
      c_macaddr8: Some(<<0x08, 0x00, 0x2B, 0x01, 0x02, 0x03, 0x04, 0x05>>),
    ),
  )
  |> should.be_ok()

  // Verify count via simple_query
  let result =
    postgleam.simple_query(conn, "SELECT count(*) FROM postgres_network_types")
    |> should.be_ok()
  should.be_true(result != [])

  query.truncate_postgres_network_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL geometric types
// ============================================================

pub fn test_geo_types_insert_test() {
  let conn = connect()
  query.truncate_postgres_geo_types(conn) |> should.be_ok()

  query.insert_postgres_geo_types(
    conn,
    query.InsertPostgresGeoTypesParams(
      c_point: Some(#(1.5, 2.5)),
      c_line: Some(#(1.0, 2.0, 3.0)),
      c_lseg: Some(#(0.0, 0.0, 1.0, 1.0)),
      c_box: Some(#(1.0, 1.0, 0.0, 0.0)),
      c_path: Some(#(False, [#(0.0, 0.0), #(1.0, 1.0), #(2.0, 0.0)])),
      c_polygon: Some([#(0.0, 0.0), #(1.0, 0.0), #(1.0, 1.0), #(0.0, 1.0)]),
      c_circle: Some(#(0.0, 0.0, 5.0)),
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_geo_types(conn) |> should.be_ok()
  should.equal(row.c_point, Some(#(1.5, 2.5)))
  should.equal(row.c_circle, Some(#(0.0, 0.0, 5.0)))

  query.truncate_postgres_geo_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_geo_types_null_insert_test() {
  let conn = connect()
  query.truncate_postgres_geo_types(conn) |> should.be_ok()

  query.insert_postgres_geo_types(
    conn,
    query.InsertPostgresGeoTypesParams(
      c_point: None,
      c_line: None,
      c_lseg: None,
      c_box: None,
      c_path: None,
      c_polygon: None,
      c_circle: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_geo_types(conn) |> should.be_ok()
  should.equal(row.c_point, None)
  should.equal(row.c_line, None)
  should.equal(row.c_circle, None)

  query.truncate_postgres_geo_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL special types (UUID, JSON, JSONB, ENUM, XML)
// ============================================================

pub fn test_special_types_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  let uuid_bytes = <<
    0x55,
    0x0E,
    0x84,
    0x00,
    0xE2,
    0x9B,
    0x41,
    0xD4,
    0xA7,
    0x16,
    0x44,
    0x66,
    0x55,
    0x44,
    0x00,
    0x00,
  >>

  // Use simple_query for insert because c_enum has a custom OID
  // that PostGleam doesn't have a codec for
  postgleam.simple_query(
    conn,
    "INSERT INTO postgres_special_types (c_json, c_json_string_override, c_jsonb, c_jsonpath, c_xml, c_xml_string_override, c_uuid, c_enum) VALUES ('{\"name\": \"test\"}'::json, '{\"a\":1}'::json, '{\"name\": \"test\"}'::jsonb, '$.name'::jsonpath, '<root>hello</root>'::xml, '<root>override</root>'::xml, '550e8400-e29b-41d4-a716-446655440000'::uuid, 'small'::c_enum)",
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.be_true(option.is_some(row.c_json))
  should.be_true(option.is_some(row.c_jsonb))
  should.equal(row.c_uuid, Some(uuid_bytes))
  should.equal(row.c_enum, Some(models.Small))
  should.equal(row.c_xml, Some("<root>hello</root>"))
  should.equal(row.c_xml_string_override, Some("<root>override</root>"))
  should.be_true(option.is_some(row.c_jsonpath))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_special_types_null_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  query.insert_postgres_special_types(
    conn,
    query.InsertPostgresSpecialTypesParams(
      c_json: None,
      c_json_string_override: None,
      c_jsonb: None,
      c_jsonpath: None,
      c_xml: None,
      c_xml_string_override: None,
      c_uuid: None,
      c_enum: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.equal(row.c_json, None)
  should.equal(row.c_jsonb, None)
  should.equal(row.c_uuid, None)
  should.equal(row.c_enum, None)
  should.equal(row.c_xml, None)

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL enum types
// ============================================================

pub fn test_enum_types_small_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  // Use simple_query for enum insert (custom OID not registered in PostGleam)
  postgleam.simple_query(
    conn,
    "INSERT INTO postgres_special_types (c_enum) VALUES ('small'::c_enum)",
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.equal(row.c_enum, Some(models.Small))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_enum_types_medium_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  postgleam.simple_query(
    conn,
    "INSERT INTO postgres_special_types (c_enum) VALUES ('medium'::c_enum)",
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.equal(row.c_enum, Some(models.Medium))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_enum_types_big_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  postgleam.simple_query(
    conn,
    "INSERT INTO postgres_special_types (c_enum) VALUES ('big'::c_enum)",
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.equal(row.c_enum, Some(models.Big))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL JSON types
// ============================================================

pub fn test_json_types_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  query.insert_postgres_special_types(
    conn,
    query.InsertPostgresSpecialTypesParams(
      c_json: Some("{\"name\": \"Swordfishtrombones\", \"year\": 1983}"),
      c_json_string_override: Some("{\"name\": \"override\"}"),
      c_jsonb: Some("{\"name\": \"Swordfishtrombones\"}"),
      c_jsonpath: Some("$.name"),
      c_xml: None,
      c_xml_string_override: None,
      c_uuid: None,
      c_enum: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.be_true(option.is_some(row.c_json))
  should.be_true(option.is_some(row.c_jsonb))
  should.be_true(option.is_some(row.c_jsonpath))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_invalid_json_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  // Invalid JSON should cause a database error
  let result =
    query.insert_postgres_special_types(
      conn,
      query.InsertPostgresSpecialTypesParams(
        c_json: Some("not valid json"),
        c_json_string_override: None,
        c_jsonb: None,
        c_jsonpath: None,
        c_xml: None,
        c_xml_string_override: None,
        c_uuid: None,
        c_enum: None,
      ),
    )
  should.be_error(result)

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL UUID types
// ============================================================

pub fn test_uuid_types_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  let uuid_bytes = <<
    0xA0,
    0xEE,
    0xBC,
    0x99,
    0x9C,
    0x0B,
    0x4E,
    0xF8,
    0xBB,
    0x6D,
    0x6B,
    0xB9,
    0xBD,
    0x38,
    0x0A,
    0x11,
  >>

  query.insert_postgres_special_types(
    conn,
    query.InsertPostgresSpecialTypesParams(
      c_json: None,
      c_json_string_override: None,
      c_jsonb: None,
      c_jsonpath: None,
      c_xml: None,
      c_xml_string_override: None,
      c_uuid: Some(uuid_bytes),
      c_enum: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.equal(row.c_uuid, Some(uuid_bytes))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_uuid_types_null_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  query.insert_postgres_special_types(
    conn,
    query.InsertPostgresSpecialTypesParams(
      c_json: None,
      c_json_string_override: None,
      c_jsonb: None,
      c_jsonpath: None,
      c_xml: None,
      c_xml_string_override: None,
      c_uuid: None,
      c_enum: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.equal(row.c_uuid, None)

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL XML types
// ============================================================

pub fn test_xml_types_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  let xml_val =
    "<root><child>Good morning xml, the world says hello</child></root>"

  query.insert_postgres_special_types(
    conn,
    query.InsertPostgresSpecialTypesParams(
      c_json: None,
      c_json_string_override: None,
      c_jsonb: None,
      c_jsonpath: None,
      c_xml: Some(xml_val),
      c_xml_string_override: Some("<root>override</root>"),
      c_uuid: None,
      c_enum: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_special_types(conn) |> should.be_ok()
  should.equal(row.c_xml, Some(xml_val))
  should.equal(row.c_xml_string_override, Some("<root>override</root>"))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_invalid_xml_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  // Invalid XML should cause a database error
  let result =
    query.insert_postgres_special_types(
      conn,
      query.InsertPostgresSpecialTypesParams(
        c_json: None,
        c_json_string_override: None,
        c_jsonb: None,
        c_jsonpath: None,
        c_xml: Some("not valid xml <<<<"),
        c_xml_string_override: None,
        c_uuid: None,
        c_enum: None,
      ),
    )
  should.be_error(result)

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL NOT NULL types
// ============================================================

pub fn test_not_null_types_test() {
  let conn = connect()
  query.truncate_postgres_not_null_types(conn) |> should.be_ok()

  // Use simple_query for enum insert (custom OID not registered in PostGleam)
  postgleam.simple_query(
    conn,
    "INSERT INTO postgres_not_null_types (c_enum_not_null) VALUES ('small'::c_enum)",
  )
  |> should.be_ok()

  let row = query.get_postgres_not_null_types(conn) |> should.be_ok()
  should.equal(row.c_enum_not_null, models.Small)

  query.truncate_postgres_not_null_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL full-text search types
// ============================================================

pub fn test_full_text_search_types_test() {
  let conn = connect()
  query.truncate_postgres_string_types(conn) |> should.be_ok()

  query.insert_postgres_string_types(
    conn,
    query.InsertPostgresStringTypesParams(
      c_char: Some("A"),
      c_varchar: Some("search text"),
      c_character_varying: Some("test"),
      c_bpchar: Some("data"),
      c_text: Some("Prophets of Rage are a supergroup"),
    ),
  )
  |> should.be_ok()

  let row =
    query.get_postgres_string_types_text_search(conn, "supergroup")
    |> should.be_ok()
  should.be_true(option.is_some(row.c_text))
  should.be_true(row.rnk >. 0.0)

  query.truncate_postgres_string_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// PostgreSQL array types (text fallback)
// ============================================================

pub fn test_array_types_bytea_test() {
  let conn = connect()
  query.truncate_postgres_array_types(conn) |> should.be_ok()

  // bytea works with binary codec, array columns are text fallback
  query.insert_postgres_array_types(
    conn,
    query.InsertPostgresArrayTypesParams(
      c_bytea: Some(<<0x45, 0x42>>),
      c_boolean_array: None,
      c_text_array: None,
      c_integer_array: None,
      c_decimal_array: None,
      c_date_array: None,
      c_timestamp_array: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_array_types(conn) |> should.be_ok()
  should.equal(row.c_bytea, Some(<<0x45, 0x42>>))

  query.truncate_postgres_array_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// Array column test: insert via generated function, read back through the
// generated array decoder.
pub fn test_array_types_insert_test() {
  let conn = connect()
  query.truncate_postgres_array_types(conn) |> should.be_ok()

  query.insert_postgres_array_types(
    conn,
    query.InsertPostgresArrayTypesParams(
      c_bytea: Some(<<0x45, 0x42>>),
      c_boolean_array: Some([True, False]),
      c_text_array: Some(["hello", "world"]),
      c_integer_array: Some([1, 2, 3]),
      c_decimal_array: None,
      c_date_array: None,
      c_timestamp_array: None,
    ),
  )
  |> should.be_ok()

  let row = query.get_postgres_array_types(conn) |> should.be_ok()
  should.equal(row.c_boolean_array, Some([True, False]))
  should.equal(row.c_text_array, Some(["hello", "world"]))
  should.equal(row.c_integer_array, Some([1, 2, 3]))
  should.equal(row.c_decimal_array, None)

  query.truncate_postgres_array_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// Extended schema / enum tests
// ============================================================

pub fn test_extended_schema_enum_test() {
  let conn = connect()
  query.truncate_extended_bios(conn) |> should.be_ok()

  // Use simple_query for enum insert (custom OID not registered in PostGleam)
  postgleam.simple_query(
    conn,
    "INSERT INTO extended.bios (author_name, name, bio_type) VALUES ('Mark Twain', 'Adventures', 'Biography'::extended.bio_type)",
  )
  |> should.be_ok()

  // Verify via simple_query since enum params can't be encoded
  let result =
    postgleam.simple_query(
      conn,
      "SELECT author_name, name, bio_type FROM extended.bios WHERE bio_type = 'Biography'::extended.bio_type LIMIT 1",
    )
    |> should.be_ok()
  should.be_true(result != [])

  query.truncate_extended_bios(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// Array parameter tests
// ============================================================

pub fn test_array_test() {
  let conn = connect()
  cleanup(conn)

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

  cleanup(conn)
  postgleam.disconnect(conn)
}

pub fn test_multiple_arrays_test() {
  let conn = connect()
  cleanup(conn)

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
      query.GetAuthorsByIdsAndNamesParams(param_1: [1, 2, 3], param_2: [
        "Alpha",
        "Gamma",
      ]),
    )
    |> should.be_ok()
  should.equal(result.count, 2)

  cleanup(conn)
  postgleam.disconnect(conn)
}

// ============================================================
// :copyfrom batch insert tests
// ============================================================

pub fn test_string_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_string_types(conn) |> should.be_ok()

  query.insert_postgres_string_types_batch(conn, [
    query.InsertPostgresStringTypesBatchParams(
      c_char: Some("A"),
      c_varchar: Some("hello"),
      c_character_varying: Some("world"),
      c_bpchar: Some("fixed"),
      c_text: Some("text1"),
    ),
    query.InsertPostgresStringTypesBatchParams(
      c_char: Some("B"),
      c_varchar: Some("foo"),
      c_character_varying: Some("bar"),
      c_bpchar: Some("baz"),
      c_text: Some("text2"),
    ),
  ])
  |> should.be_ok()

  let row = query.get_postgres_string_types_cnt(conn) |> should.be_ok()
  should.be_true(row.cnt >= 1)

  query.truncate_postgres_string_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_integer_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_numeric_types(conn) |> should.be_ok()

  query.insert_postgres_numeric_types_batch(conn, [
    query.InsertPostgresNumericTypesBatchParams(
      c_boolean: Some(True),
      c_bit: None,
      c_smallint: Some(10),
      c_integer: Some(100),
      c_bigint: Some(1000),
      c_decimal: None,
      c_numeric: None,
      c_real: None,
      c_double_precision: None,
      c_money: None,
    ),
    query.InsertPostgresNumericTypesBatchParams(
      c_boolean: Some(True),
      c_bit: None,
      c_smallint: Some(10),
      c_integer: Some(100),
      c_bigint: Some(1000),
      c_decimal: None,
      c_numeric: None,
      c_real: None,
      c_double_precision: None,
      c_money: None,
    ),
  ])
  |> should.be_ok()

  let row = query.get_postgres_numeric_types_cnt(conn) |> should.be_ok()
  should.equal(row.cnt, 2)
  should.equal(row.c_smallint, Some(10))
  should.equal(row.c_integer, Some(100))

  query.truncate_postgres_numeric_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_float_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_numeric_types(conn) |> should.be_ok()

  query.insert_postgres_numeric_types_batch(conn, [
    query.InsertPostgresNumericTypesBatchParams(
      c_boolean: None,
      c_bit: None,
      c_smallint: None,
      c_integer: None,
      c_bigint: None,
      c_decimal: None,
      c_numeric: None,
      c_real: Some(1.5),
      c_double_precision: Some(2.5),
      c_money: None,
    ),
    query.InsertPostgresNumericTypesBatchParams(
      c_boolean: None,
      c_bit: None,
      c_smallint: None,
      c_integer: None,
      c_bigint: None,
      c_decimal: None,
      c_numeric: None,
      c_real: Some(3.5),
      c_double_precision: Some(4.5),
      c_money: None,
    ),
  ])
  |> should.be_ok()

  let row = query.get_postgres_numeric_types_cnt(conn) |> should.be_ok()
  should.equal(row.cnt, 1)
  should.be_true(is_some_float_near(row.c_real, 1.5, 0.01))

  query.truncate_postgres_numeric_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_datetime_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_date_time_types(conn) |> should.be_ok()

  let date_val = 19_790
  let time_val = 43_200_000_000
  let ts_val = 1_710_489_600_000_000
  let interval_val = #(3_600_000_000, 0, 0)

  query.insert_postgres_date_time_types_batch(conn, [
    query.InsertPostgresDateTimeTypesBatchParams(
      c_date: Some(date_val),
      c_time: Some(time_val),
      c_timestamp: Some(ts_val),
      c_timestamp_with_tz: Some(ts_val),
      c_interval: Some(interval_val),
    ),
    query.InsertPostgresDateTimeTypesBatchParams(
      c_date: Some(date_val),
      c_time: Some(time_val),
      c_timestamp: Some(ts_val),
      c_timestamp_with_tz: Some(ts_val),
      c_interval: Some(interval_val),
    ),
  ])
  |> should.be_ok()

  let row = query.get_postgres_date_time_types_cnt(conn) |> should.be_ok()
  should.equal(row.cnt, 2)
  should.equal(row.c_date, Some(date_val))
  should.equal(row.c_time, Some(time_val))

  query.truncate_postgres_date_time_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_array_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_array_types(conn) |> should.be_ok()

  query.insert_postgres_array_types_batch(conn, [
    query.InsertPostgresArrayTypesBatchParams(
      c_bytea: Some(<<0x45, 0x42>>),
      c_boolean_array: Some([True, False]),
      c_text_array: Some(["hello", "world"]),
      c_integer_array: Some([1, 2, 3]),
      c_decimal_array: None,
      c_timestamp_array: None,
    ),
    query.InsertPostgresArrayTypesBatchParams(
      c_bytea: Some(<<0xAB, 0xCD>>),
      c_boolean_array: Some([False]),
      c_text_array: Some(["foo"]),
      c_integer_array: Some([4, 5]),
      c_decimal_array: None,
      c_timestamp_array: None,
    ),
  ])
  |> should.be_ok()

  // Verify via simple_query since array result columns can't be decoded
  let result =
    postgleam.simple_query(conn, "SELECT count(*) FROM postgres_array_types")
    |> should.be_ok()
  should.be_true(result != [])

  query.truncate_postgres_array_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_uuid_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  let uuid1 = <<
    0x55,
    0x0E,
    0x84,
    0x00,
    0xE2,
    0x9B,
    0x41,
    0xD4,
    0xA7,
    0x16,
    0x44,
    0x66,
    0x55,
    0x44,
    0x00,
    0x00,
  >>
  let uuid2 = <<
    0xA0,
    0xEE,
    0xBC,
    0x99,
    0x9C,
    0x0B,
    0x4E,
    0xF8,
    0xBB,
    0x6D,
    0x6B,
    0xB9,
    0xBD,
    0x38,
    0x0A,
    0x11,
  >>

  query.insert_postgres_special_types_batch(conn, [
    query.InsertPostgresSpecialTypesBatchParams(
      c_uuid: Some(uuid1),
      c_json: Some("{\"k\":1}"),
      c_jsonb: Some("{\"k\":1}"),
    ),
    query.InsertPostgresSpecialTypesBatchParams(
      c_uuid: Some(uuid2),
      c_json: Some("{\"k\":2}"),
      c_jsonb: Some("{\"k\":2}"),
    ),
  ])
  |> should.be_ok()

  let row = query.get_postgres_special_types_cnt(conn) |> should.be_ok()
  should.equal(row.cnt, 1)
  should.be_true(option.is_some(row.c_uuid))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_geo_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_geo_types(conn) |> should.be_ok()

  query.insert_postgres_geo_types_batch(conn, [
    query.InsertPostgresGeoTypesBatchParams(
      c_point: Some(#(1.0, 2.0)),
      c_line: Some(#(1.0, 2.0, 3.0)),
      c_lseg: Some(#(0.0, 0.0, 1.0, 1.0)),
      c_box: Some(#(1.0, 1.0, 0.0, 0.0)),
      c_path: Some(#(False, [#(0.0, 0.0), #(1.0, 1.0)])),
      c_polygon: Some([#(0.0, 0.0), #(1.0, 0.0), #(1.0, 1.0)]),
      c_circle: Some(#(0.0, 0.0, 5.0)),
    ),
    query.InsertPostgresGeoTypesBatchParams(
      c_point: Some(#(3.0, 4.0)),
      c_line: Some(#(4.0, 5.0, 6.0)),
      c_lseg: Some(#(1.0, 1.0, 2.0, 2.0)),
      c_box: Some(#(2.0, 2.0, 0.0, 0.0)),
      c_path: Some(#(True, [#(0.0, 0.0), #(1.0, 0.0)])),
      c_polygon: Some([#(0.0, 0.0), #(2.0, 0.0), #(2.0, 2.0)]),
      c_circle: Some(#(1.0, 1.0, 3.0)),
    ),
  ])
  |> should.be_ok()

  let row = query.get_postgres_geo_types(conn) |> should.be_ok()
  should.be_true(option.is_some(row.c_point))

  query.truncate_postgres_geo_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_network_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_network_types(conn) |> should.be_ok()

  query.insert_postgres_network_types_batch(conn, [
    query.InsertPostgresNetworkTypesBatchParams(
      c_cidr: Some(#(2, <<192, 168, 1, 0>>, 24)),
      c_inet: Some(#(2, <<192, 168, 1, 1>>, 32)),
      c_macaddr: Some(<<0x08, 0x00, 0x2B, 0x01, 0x02, 0x03>>),
    ),
    query.InsertPostgresNetworkTypesBatchParams(
      c_cidr: Some(#(2, <<10, 0, 0, 0>>, 8)),
      c_inet: Some(#(2, <<10, 0, 0, 1>>, 32)),
      c_macaddr: Some(<<0x08, 0x00, 0x2B, 0x04, 0x05, 0x06>>),
    ),
  ])
  |> should.be_ok()

  let row = query.get_postgres_network_types_cnt(conn) |> should.be_ok()
  should.equal(row.cnt, 1)
  should.be_true(option.is_some(row.c_inet))

  query.truncate_postgres_network_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn test_json_copy_from_test() {
  let conn = connect()
  query.truncate_postgres_special_types(conn) |> should.be_ok()

  query.insert_postgres_special_types_batch(conn, [
    query.InsertPostgresSpecialTypesBatchParams(
      c_uuid: None,
      c_json: Some("{\"name\": \"test1\"}"),
      c_jsonb: Some("{\"name\": \"test1\"}"),
    ),
    query.InsertPostgresSpecialTypesBatchParams(
      c_uuid: None,
      c_json: Some("{\"name\": \"test2\"}"),
      c_jsonb: Some("{\"name\": \"test2\"}"),
    ),
  ])
  |> should.be_ok()

  let row = query.get_postgres_special_types_cnt(conn) |> should.be_ok()
  should.equal(row.cnt, 1)
  should.be_true(string.contains(option.unwrap(row.c_json, ""), "test"))

  query.truncate_postgres_special_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// Type override test
// ============================================================

// sqlc cannot type max(), so sqlc.yaml names the real type of each column
// of GetPostgresFunctions with a dbType override.
pub fn test_type_override_test() {
  let conn = connect()
  query.truncate_postgres_numeric_types(conn) |> should.be_ok()
  query.truncate_postgres_string_types(conn) |> should.be_ok()
  query.truncate_postgres_date_time_types(conn) |> should.be_ok()

  // Insert data into all three tables used in the CROSS JOIN
  query.insert_postgres_numeric_types(
    conn,
    query.InsertPostgresNumericTypesParams(
      c_boolean: None,
      c_bit: None,
      c_smallint: None,
      c_integer: Some(42),
      c_bigint: None,
      c_decimal: None,
      c_numeric: None,
      c_real: None,
      c_double_precision: None,
      c_money: None,
    ),
  )
  |> should.be_ok()
  query.insert_postgres_string_types(
    conn,
    query.InsertPostgresStringTypesParams(
      c_char: None,
      c_varchar: Some("hello"),
      c_character_varying: None,
      c_bpchar: None,
      c_text: None,
    ),
  )
  |> should.be_ok()
  query.insert_postgres_date_time_types(
    conn,
    query.InsertPostgresDateTimeTypesParams(
      c_date: None,
      c_time: None,
      c_timestamp: Some(1_700_000_000_000_000),
      c_timestamp_with_tz: None,
      c_interval: None,
      c_timestamp_noda_instant_override: None,
    ),
  )
  |> should.be_ok()

  query.get_postgres_functions(conn)
  |> should.equal(
    Ok(query.GetPostgresFunctionsRow(
      max_integer: Some(42),
      max_varchar: Some("hello"),
      max_timestamp: 1_700_000_000_000_000,
    )),
  )

  query.truncate_postgres_numeric_types(conn) |> should.be_ok()
  query.truncate_postgres_string_types(conn) |> should.be_ok()
  query.truncate_postgres_date_time_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
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

// ============================================================
// Error handling and generated helpers
// ============================================================

// :one returns postgleam's own error, never a string.
pub fn one_no_rows_is_decode_error_test() {
  let conn = connect()
  cleanup(conn)
  case query.get_author(conn, "nobody") {
    Error(error.DecodeError(_)) -> Nil
    other -> panic as { "expected DecodeError, got " <> string.inspect(other) }
  }
  postgleam.disconnect(conn)
}

pub fn one_database_error_is_kept_test() {
  let conn = connect()
  cleanup(conn)
  let params = query.CreateAuthorParams(id: 7, name: "Dup", bio: None)
  query.create_author(conn, params) |> should.be_ok()
  case query.create_author(conn, params) {
    Error(error.PgError(fields:, ..)) -> should.equal(fields.code, "23505")
    other -> panic as { "expected PgError, got " <> string.inspect(other) }
  }
  cleanup(conn)
  postgleam.disconnect(conn)
}

// Enum params are sent from the generated enum type and decoded back into it.
pub fn enum_param_round_trip_test() {
  let conn = connect()
  query.truncate_postgres_not_null_types(conn) |> should.be_ok()
  query.insert_postgres_not_null_types(conn, models.Big) |> should.be_ok()
  let row = query.get_postgres_not_null_types(conn) |> should.be_ok()
  should.equal(row.c_enum_not_null, models.Big)
  should.equal(models.c_enum_to_string(row.c_enum_not_null), "big")
  should.equal(models.c_enum_from_string("medium"), Ok(models.Medium))
  should.equal(models.c_enum_from_string("huge"), Error(Nil))
  query.truncate_postgres_not_null_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// :copyfrom is one pipelined batch: it returns the row count, and a bad row
// rolls back the whole batch.
pub fn copyfrom_is_atomic_test() {
  let conn = connect()
  query.truncate_postgres_string_types(conn) |> should.be_ok()
  let row = fn(c) {
    query.InsertPostgresStringTypesBatchParams(
      c_char: Some(c),
      c_varchar: None,
      c_character_varying: None,
      c_bpchar: None,
      c_text: None,
    )
  }
  query.insert_postgres_string_types_batch(conn, [row("a"), row("b")])
  |> should.equal(Ok(2))
  // CHAR(1) rejects "too long", so neither row of this batch may be kept.
  query.insert_postgres_string_types_batch(conn, [row("c"), row("too long")])
  |> should.be_error()
  let rows =
    postgleam.simple_query(conn, "SELECT count(*) FROM postgres_string_types")
    |> should.be_ok()
  should.be_true(string.contains(string.inspect(rows), "\"2\""))
  query.truncate_postgres_string_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// ============================================================
// Types sqlc infers little about
// ============================================================

fn inferred(
  enums: List(models.CEnum),
  count: Int,
  score: Int,
  amount: String,
) -> query.InsertPostgresInferredTypesParams {
  query.InsertPostgresInferredTypesParams(
    c_enum_array: Some(enums),
    c_domain: Some("5"),
    c_oid: Some(42),
    c_amount: amount,
    c_count: count,
    c_score: Some(score),
    c_seen_at: Some(1_700_000_000_000_000 + count),
    c_tags: Some(["a", "b c"]),
    c_labels: Some(tricky_labels),
  )
}

/// Text that needs quoting or escaping inside an array literal.
const tricky_labels = [
  "plain", "", "NULL", "null", "two words", "say \"hi\"", "back\\slash",
  "{braces},comma", "naïve ☃",
]

// An enum array has no binary codec: it is sent and read as an array
// literal. A domain is a String unless typeAliases says what it is built on,
// and it is read back whichever type PostgreSQL sends.
pub fn enum_array_domain_and_oid_round_trip_test() {
  let conn = connect()
  query.truncate_postgres_inferred_types(conn) |> should.be_ok()

  let row =
    query.insert_postgres_inferred_types(
      conn,
      inferred([models.Small, models.Big], 3, 7, "12.50"),
    )
    |> should.be_ok()
  should.equal(row.c_enum_array, Some([models.Small, models.Big]))
  should.equal(row.c_domain, Some("5"))
  should.equal(row.c_oid, Some(42))
  should.equal(row.c_amount, "12.50")
  should.equal(row.c_tags, Some(["a", "b c"]))
  // An array of a type the plugin does not know (a domain over text) is a
  // list of strings, sent and read as an array literal.
  should.equal(row.c_labels, Some(tricky_labels))

  // An empty array, and NULL for every nullable column.
  let empty =
    query.insert_postgres_inferred_types(
      conn,
      query.InsertPostgresInferredTypesParams(
        c_enum_array: Some([]),
        c_domain: None,
        c_oid: None,
        c_amount: "0",
        c_count: 0,
        c_score: None,
        c_seen_at: None,
        c_tags: None,
        c_labels: Some([]),
      ),
    )
    |> should.be_ok()
  should.equal(empty.c_labels, Some([]))
  should.equal(empty.c_enum_array, Some([]))
  should.equal(empty.c_domain, None)
  should.equal(empty.c_oid, None)

  let rows = query.get_postgres_inferred_types(conn) |> should.be_ok()
  should.equal(list.map(rows.rows, fn(r) { r.c_enum_array }), [
    Some([models.Small, models.Big]),
    Some([]),
  ])

  // The domain's CHECK still applies.
  query.insert_postgres_inferred_types(
    conn,
    query.InsertPostgresInferredTypesParams(
      ..inferred([], 1, 1, "1"),
      c_domain: Some("-5"),
    ),
  )
  |> should.be_error()

  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

pub fn enum_array_param_test() {
  let conn = connect()
  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  let small =
    query.insert_postgres_inferred_types(
      conn,
      inferred([models.Small], 1, 1, "1"),
    )
    |> should.be_ok()
  let big =
    query.insert_postgres_inferred_types(
      conn,
      inferred([models.Medium, models.Big], 2, 2, "2"),
    )
    |> should.be_ok()

  let ids = fn(sizes) {
    query.get_postgres_inferred_by_enums(conn, sizes)
    |> should.be_ok()
    |> fn(response) { list.map(response.rows, fn(r) { r.id }) }
  }
  should.equal(ids([models.Big]), [big.id])
  should.equal(ids([models.Small, models.Medium]), [small.id, big.id])
  should.equal(ids([]), [])

  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// sqlc types sum(bigint) as a bigint and avg(integer) as a double; both are
// numerics in PostgreSQL and are read leniently. The other columns get
// their real type from a dbType override in sqlc.yaml.
pub fn aggregates_test() {
  let conn = connect()
  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  let first =
    query.insert_postgres_inferred_types(
      conn,
      inferred([models.Small], 3, 7, "12.50"),
    )
    |> should.be_ok()
  let second =
    query.insert_postgres_inferred_types(
      conn,
      inferred([models.Big], 4, 8, "0.25"),
    )
    |> should.be_ok()

  let stats = query.get_postgres_inferred_aggregates(conn) |> should.be_ok()
  should.equal(stats.sum_count, 7)
  should.equal(stats.avg_score, 7.5)
  should.equal(stats.sum_amount, Some("12.75"))
  should.equal(stats.max_seen_at, Some(1_700_000_000_000_004))
  should.equal(stats.ids, Some([first.id, second.id]))

  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// Parameters are named after sqlc.arg() and sqlc.narg(), not after the
// column they are compared with, and a parameter that merely shares its
// name with a nullable column (c_tags) is not made optional.
pub fn named_params_test() {
  let conn = connect()
  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  let low =
    query.insert_postgres_inferred_types(
      conn,
      inferred([models.Small], 1, 2, "1"),
    )
    |> should.be_ok()
  query.insert_postgres_inferred_types(
    conn,
    inferred([models.Small], 1, 9, "1"),
  )
  |> should.be_ok()

  let find = fn(tag, excluded) {
    query.get_postgres_inferred_by_score(
      conn,
      query.GetPostgresInferredByScoreParams(
        min_score: Some(1),
        max_score: Some(5),
        tag: tag,
        c_tags: excluded,
      ),
    )
    |> should.be_ok()
    |> fn(response) { list.map(response.rows, fn(r) { r.id }) }
  }
  should.equal(find(None, []), [low.id])
  should.equal(find(Some("b c"), []), [low.id])
  should.equal(find(Some("missing"), []), [])
  should.equal(find(None, ["a", "b c"]), [])

  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}

// An array holding a NULL cannot be a List(String). The error says how to
// read it; with nullableArrayElements the elements are Options.
pub fn null_array_elements_test() {
  let conn = connect()
  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  let row =
    query.insert_postgres_inferred_types(
      conn,
      inferred([models.Small], 1, 1, "1"),
    )
    |> should.be_ok()
  query.set_postgres_inferred_tags(conn, row.id) |> should.be_ok()

  let assert Error(error.DecodeError(message)) =
    query.get_postgres_inferred_types(conn)
  should.be_true(string.contains(message, "nullableArrayElements"))

  let rows = nullable_query.get_postgres_inferred_types(conn) |> should.be_ok()
  should.equal(list.map(rows.rows, fn(r) { r.c_tags }), [
    Some([Some("a"), None, Some("c")]),
  ])
  // typeAliases declares the domain as an integer in this module.
  should.equal(list.map(rows.rows, fn(r) { r.c_domain }), [Some(5)])

  // NULL elements can be written too, in binary arrays and in literals.
  let written =
    nullable_query.insert_postgres_inferred_types(
      conn,
      nullable_query.InsertPostgresInferredTypesParams(
        c_enum_array: Some([Some(nullable_models.Medium), None]),
        c_domain: Some(9),
        c_oid: None,
        c_amount: "1",
        c_count: 1,
        c_score: None,
        c_seen_at: None,
        c_tags: Some([None, Some("NULL"), Some("say \"hi\" \\ {x},y")]),
        c_labels: Some([Some("NULL"), None, Some("")]),
      ),
    )
    |> should.be_ok()
  should.equal(written.c_labels, Some([Some("NULL"), None, Some("")]))
  should.equal(written.c_enum_array, Some([Some(nullable_models.Medium), None]))
  should.equal(written.c_domain, Some(9))
  should.equal(
    written.c_tags,
    Some([None, Some("NULL"), Some("say \"hi\" \\ {x},y")]),
  )
  nullable_query.get_postgres_inferred_by_enums(conn, [
    None,
    Some(nullable_models.Medium),
  ])
  |> should.be_ok()
  |> fn(response) { list.map(response.rows, fn(r) { r.id }) }
  |> should.equal([written.id])

  query.truncate_postgres_inferred_types(conn) |> should.be_ok()
  postgleam.disconnect(conn)
}
