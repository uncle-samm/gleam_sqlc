use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::driver::traits::{Driver, path_alias, uses_module};
use crate::driver::type_map::{
    GleamType, ParamExpr, ResolvedType, Shape, TypeContext, tuple_pattern, value_fn,
};
use crate::options::Options;
use crate::plugin::plugin::Column;
use crate::utils::CodeWriter;

/// Generates code for [postgleam](https://hex.pm/packages/postgleam).
///
/// Calls look like `postgleam.query_with(conn, sql, params, decoder)`. The
/// `module` option swaps `postgleam` for any module with the same functions,
/// such as `postgleam/pool` or a wrapper of your own.
pub struct PostgresDriver {
    /// Full import path, e.g., "postgleam/pool" or "db/pg"
    import: String,
    /// Module alias (last segment), e.g., "pool" or "pg"
    alias: String,
    /// Full decode import path, e.g., "postgleam/decode"
    decode_import: String,
    /// Map uuid → String instead of BitArray
    uuid_as_string: bool,
    /// Type array elements as Option.
    nullable_array_elements: bool,
    /// User-declared aliases (lowercase) for types sqlc does not describe.
    type_aliases: BTreeMap<String, String>,
    /// Unknown types already warned about, so each is reported once.
    warned: RefCell<BTreeSet<String>>,
}

impl PostgresDriver {
    pub fn new(options: &Options) -> Self {
        let import = options
            .module
            .clone()
            .unwrap_or_else(|| "postgleam".to_string());
        let alias = path_alias(&import);
        let decode_import = options.decode_module.clone().unwrap_or_else(|| {
            if import == "postgleam/pool" {
                "postgleam/decode".to_string()
            } else {
                format!("{import}/decode")
            }
        });
        Self {
            import,
            alias,
            decode_import,
            uuid_as_string: options.uuid_as_string,
            nullable_array_elements: options.nullable_array_elements,
            type_aliases: options
                .type_aliases
                .iter()
                .map(|(k, v)| (k.to_lowercase(), v.clone()))
                .collect(),
            warned: RefCell::new(BTreeSet::new()),
        }
    }

    /// The type behind a user-declared alias, if there is one.
    fn alias_target(&self, schema: &str, type_name: &str) -> Option<&str> {
        let name = type_name.to_lowercase();
        let qualified = format!("{}.{name}", schema.to_lowercase());
        self.type_aliases
            .get(&qualified)
            .or_else(|| self.type_aliases.get(&name))
            .map(String::as_str)
    }

    /// An enum from the catalog or a type postgleam has a codec for.
    fn enum_or_builtin(
        &self,
        schema: &str,
        type_name: &str,
        ctx: &TypeContext,
    ) -> Option<GleamType> {
        let key = if schema.is_empty() || schema == "public" {
            type_name.to_string()
        } else {
            format!("{schema}.{type_name}")
        };
        match (ctx.enums.get(&key), &ctx.enum_qualifier) {
            // Enums have no binary codec: values travel as text, arrays as
            // array literals.
            (Some(info), Some(q)) => Some(GleamType {
                literal_fn: Some(format!("{q}{}_to_string", info.snake)),
                ..GleamType::simple(
                    &format!("{q}{}", info.type_name),
                    &format!("encode_{}", info.snake),
                    &format!("decode_{}", info.snake),
                )
            }),
            // Without a models module an enum is a plain String.
            (Some(_), None) => Some(text_only("decode.text")),
            (None, _) => pg_known_type(type_name, self.uuid_as_string),
        }
    }

    /// A type the user aliased, typically a domain. PostgreSQL describes a
    /// parameter by the domain's own type, which postgleam has no codec for,
    /// so parameters are sent as text. Results arrive as the base type.
    fn aliased(&self, type_name: &str, target: &str, mut base: GleamType) -> GleamType {
        let text_fn = match base.param_fn.as_str() {
            "value.Integer" | "value.Oid" => "int_as_text",
            "value.Float" => "float_as_text",
            "value.Boolean" => "bool_as_text",
            "value.Text" | "value.Numeric" | "value.Json" | "value.Jsonb" | "value.Xml"
            | "value.Jsonpath" => "value.Text",
            "value.Uuid" if base.uuid_string => "value.Text",
            // Enums are already sent as text.
            other if other.starts_with("encode_") => other,
            other => {
                if self
                    .warned
                    .borrow_mut()
                    .insert(format!("alias {type_name}"))
                {
                    eprintln!(
                        "warning: typeAliases maps '{type_name}' to '{target}': values of this type can be read, \
                         but not sent as parameters (postgleam has no text form for {target})."
                    );
                }
                other
            }
        };
        base.param_fn = text_fn.to_string();
        base.uuid_string = false;
        base
    }

    fn base_type(&self, col: &Column, ctx: &TypeContext) -> GleamType {
        let type_id = col.r#type.as_ref();
        let type_name = type_id.map(|t| t.name.as_str()).unwrap_or("text");
        let schema = type_id.map(|t| t.schema.as_str()).unwrap_or("");
        let known = match self.alias_target(schema, type_name) {
            // An array of an aliased type arrives as an array literal, so
            // its elements are read as text.
            Some(_) if col.is_array => Some(text_only("decode_as_text")),
            Some(target) => self
                .enum_or_builtin("", target, ctx)
                .map(|base| self.aliased(type_name, target, base)),
            None => self.enum_or_builtin(schema, type_name, ctx),
        };
        known.unwrap_or_else(|| {
            if self.warned.borrow_mut().insert(type_name.to_string()) {
                eprintln!(
                    "warning: unknown PostgreSQL type '{type_name}' (e.g. column '{}'): typed as String. \
                     If that is wrong, name the type it is built on with the typeAliases option, \
                     or override the column with dbType.",
                    col.name
                );
            }
            text_only("decode_as_text")
        })
    }
}

/// A type sent and read as text: it has no binary codec in postgleam.
fn text_only(decoder: &str) -> GleamType {
    GleamType {
        literal_fn: Some(String::new()),
        ..GleamType::simple("String", "value.Text", decoder)
    }
}

/// Call a value constructor of the given arity on `var` (tuples are destructured).
fn call_value(fn_name: &str, arity: u8, var: &str) -> String {
    if arity <= 1 {
        format!("{fn_name}({var})")
    } else {
        let (pattern, args) = tuple_pattern(arity);
        format!("case {var} {{ {pattern} -> {fn_name}({args}) }}")
    }
}

/// A `value.Value` holding the list `var` as a PostgreSQL array.
fn array_value(
    elem_fn: &str,
    arity: u8,
    elem_nullable: bool,
    literal_fn: Option<&str>,
    var: &str,
) -> String {
    match (literal_fn, elem_nullable) {
        (None, false) => format!(
            "value.Array(list.map({var}, fn(e) {{ Some({}) }}))",
            call_value(elem_fn, arity, "e")
        ),
        (None, true) => format!(
            "value.Array(list.map({var}, option.map(_, {})))",
            value_fn(elem_fn, arity)
        ),
        (Some(""), false) => format!("array_literal(list.map({var}, Some))"),
        (Some(""), true) => format!("array_literal({var})"),
        (Some(f), false) => format!("array_literal(list.map({var}, fn(e) {{ Some({f}(e)) }}))"),
        (Some(f), true) => format!("array_literal(list.map({var}, option.map(_, {f})))"),
    }
}

impl Driver for PostgresDriver {
    fn module_name(&self) -> &str {
        &self.alias
    }

    fn import_path(&self) -> &str {
        &self.import
    }

    fn decode_import_path(&self) -> &str {
        &self.decode_import
    }

    fn resolve_column_type(&self, col: &Column, ctx: &TypeContext) -> ResolvedType {
        let mut base = self.base_type(col, ctx);
        // sqlc picks an aggregate's type without looking at its argument:
        // sum(bigint) and avg(integer) are numerics in PostgreSQL, but typed
        // as bigint and double precision. Read such results leniently.
        if col.is_func_call && !col.is_array {
            match base.decoder_fn.as_str() {
                "decode.int" => base.decoder_fn = "decode_number_as_int".into(),
                "decode.float" => base.decoder_fn = "decode_number_as_float".into(),
                _ => {}
            }
        }
        let shape = Shape {
            nullable: !col.not_null,
            array: col.is_array,
            elem_nullable: col.is_array && self.nullable_array_elements,
        };
        let array_decoder = if shape.elem_nullable {
            format!("decode_array(decode.optional({}))", base.decoder_fn)
        } else {
            format!("decode_array({})", base.decoder_fn)
        };
        ResolvedType::from_base(base, shape, Some(array_decoder))
    }

    fn knows_type(&self, type_name: &str, ctx: &TypeContext) -> bool {
        self.alias_target("", type_name).is_some()
            || self.enum_or_builtin("", type_name, ctx).is_some()
    }

    fn override_base(&self, type_name: &str) -> GleamType {
        match type_name {
            "String" => GleamType::simple("String", "value.Text", "decode.text"),
            "Int" => GleamType::simple("Int", "value.Integer", "decode.int"),
            "Float" => GleamType::simple("Float", "value.Float", "decode.float"),
            "Bool" => GleamType::simple("Bool", "value.Boolean", "decode.bool"),
            "BitArray" => GleamType::simple("BitArray", "value.Bytea", "decode.bytea"),
            other => {
                unreachable!("override type '{other}' should have been rejected by Options::parse")
            }
        }
    }

    fn render_param(&self, expr: &ParamExpr, var: &str) -> String {
        match expr {
            ParamExpr::Value {
                fn_name,
                arity,
                nullable: false,
            } => {
                format!("Some({})", call_value(fn_name, *arity, var))
            }
            ParamExpr::Value {
                fn_name,
                arity,
                nullable: true,
            } => {
                format!("option.map({var}, {})", value_fn(fn_name, *arity))
            }
            ParamExpr::Array {
                elem_fn,
                arity,
                nullable,
                elem_nullable,
                literal_fn,
            } => {
                let array = |v: &str| {
                    array_value(elem_fn, *arity, *elem_nullable, literal_fn.as_deref(), v)
                };
                if *nullable {
                    format!("option.map({var}, fn(l) {{ {} }})", array("l"))
                } else {
                    format!("Some({})", array(var))
                }
            }
            // Prepared by `prepare_param`; `var` is already the bound Param.
            ParamExpr::UuidString { .. } => var.to_string(),
        }
    }

    fn prepare_param(&self, expr: &ParamExpr, var: &str) -> Option<String> {
        let ParamExpr::UuidString {
            nullable,
            array,
            elem_nullable,
        } = expr
        else {
            return None;
        };
        let single = |v: &str| format!("uuid_param({v}) |> result.map(Some)");
        let many = |v: &str| {
            if *elem_nullable {
                format!(
                    "list.try_map({v}, fn(e) {{ case e {{ option.Some(s) -> uuid_param(s) |> result.map(Some)  option.None -> Ok(None) }} }}) |> result.map(fn(vs) {{ Some(value.Array(vs)) }})"
                )
            } else {
                format!(
                    "list.try_map({v}, uuid_param) |> result.map(fn(vs) {{ Some(value.Array(list.map(vs, Some))) }})"
                )
            }
        };
        let inner = if *array { many("u") } else { single("u") };
        Some(match nullable {
            false => {
                if *array {
                    many(var)
                } else {
                    single(var)
                }
            }
            true => format!("case {var} {{ option.Some(u) -> {inner}  option.None -> Ok(None) }}"),
        })
    }

    fn render_slice_param(&self, _expr: &ParamExpr, var: &str) -> String {
        // sqlc only emits sqlc.slice() for MySQL and SQLite.
        unreachable!("sqlc.slice() is not supported by the PostgreSQL engine (param {var})")
    }

    fn extra_imports(&self, body: &str) -> Vec<String> {
        let mut imports = Vec::new();
        if uses_module(body, "error") {
            imports.push("postgleam/error".to_string());
        }
        if uses_module(body, "value") {
            imports.push("postgleam/value".to_string());
        }
        imports
    }

    fn write_helpers(&self, body: &str, ctx: &TypeContext, w: &mut CodeWriter) {
        let mut wrote = false;
        let mut sep = |w: &mut CodeWriter| {
            if wrote {
                w.blank();
            }
            wrote = true;
        };
        if body.contains("affected_rows(") {
            sep(w);
            w.line("/// Parse the affected row count from a PostgreSQL command tag such as");
            w.line("/// \"UPDATE 5\", \"DELETE 2\" or \"INSERT 0 1\".");
            w.line("fn affected_rows(tag: String) -> Result(Int, error.Error) {");
            w.line("  case list.last(string.split(tag, \" \")) {");
            w.line("    Ok(count) ->");
            w.line("      int.parse(count)");
            w.line("      |> result.replace_error(error.DecodeError(\"unexpected command tag: \" <> tag))");
            w.line("    Error(Nil) -> Error(error.DecodeError(\"empty command tag\"))");
            w.line("  }");
            w.line("}");
        }
        if body.contains("uuid_param") {
            sep(w);
            w.line("fn uuid_param(uuid: String) -> Result(value.Value, error.Error) {");
            w.line("  value.uuid_from_string(uuid)");
            w.line("  |> result.replace_error(error.EncodeError(\"invalid UUID: \" <> uuid))");
            w.line("}");
        }
        let mut block = |w: &mut CodeWriter, text: &str| {
            sep(w);
            for line in text.trim_matches('\n').lines() {
                w.line(line);
            }
        };
        if body.contains("array_literal(") {
            block(w, ARRAY_LITERAL);
        }
        if body.contains("decode_array(") {
            block(w, DECODE_ARRAY);
        }
        for (name, text) in [
            ("decode_number_as_int", DECODE_NUMBER_AS_INT),
            ("decode_number_as_float", DECODE_NUMBER_AS_FLOAT),
            ("decode_as_text", DECODE_AS_TEXT),
            ("decode_oid", DECODE_OID),
            ("int_as_text", INT_AS_TEXT),
            ("float_as_text", FLOAT_AS_TEXT),
            ("bool_as_text", BOOL_AS_TEXT),
        ] {
            if uses_fn(body, name) {
                block(w, text);
            }
        }
        let Some(q) = &ctx.enum_qualifier else {
            return;
        };
        let mut enums: Vec<_> = ctx.enums.values().collect();
        enums.sort_by(|a, b| a.snake.cmp(&b.snake));
        enums.dedup_by(|a, b| a.snake == b.snake);
        for info in enums {
            let t = format!("{q}{}", info.type_name);
            let snake = &info.snake;
            if uses_fn(body, &format!("encode_{snake}")) {
                sep(w);
                w.writef(format_args!("fn encode_{snake}(v: {t}) -> value.Value {{"));
                w.writef(format_args!("  value.Text({q}{snake}_to_string(v))"));
                w.line("}");
            }
            if uses_fn(body, &format!("decode_{snake}")) {
                sep(w);
                w.writef(format_args!(
                    "fn decode_{snake}(v: Option(value.Value)) -> Result({t}, error.Error) {{"
                ));
                w.line("  use s <- result.try(decode.text(v))");
                w.writef(format_args!("  case {q}{snake}_from_string(s) {{"));
                w.line("    Ok(x) -> Ok(x)");
                w.writef(format_args!(
                    "    Error(Nil) -> Error(error.DecodeError(\"unexpected {snake} value: \" <> s))"
                ));
                w.line("  }");
                w.line("}");
            }
        }
    }

    fn write_one_call(&self, sql: &str, params: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        w.writef(format_args!(
            "{m}.query_one(conn, {sql}, {params}, decoder)"
        ));
    }

    fn write_many_call(&self, sql: &str, params: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        w.writef(format_args!(
            "{m}.query_with(conn, {sql}, {params}, decoder)"
        ));
    }

    fn write_exec_call(&self, sql: &str, params: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        w.writef(format_args!("{m}.query(conn, {sql}, {params})"));
        w.line("|> result.replace(Nil)");
    }

    fn write_execrows_call(&self, sql: &str, params: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        w.writef(format_args!("{m}.query(conn, {sql}, {params})"));
        w.line("|> result.try(fn(r) { affected_rows(r.tag) })");
    }

    fn write_copyfrom_call(&self, sql: &str, param_sets: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        // One pipeline, one Sync: PostgreSQL runs it as a single implicit
        // transaction, so either every row is inserted or none is.
        w.writef(format_args!("{m}.query_batch(conn, {sql}, {param_sets})"));
        w.line("|> result.map(list.length)");
    }
}

const ARRAY_LITERAL: &str = r#"
/// Arrays of enums and of other types without a binary codec are sent as an
/// array literal such as `{"a","b",NULL}`.
fn array_literal(items: List(Option(String))) -> value.Value {
  let quoted =
    list.map(items, fn(item) {
      case item {
        Some(s) ->
          "\""
          <> string.replace(string.replace(s, "\\", "\\\\"), "\"", "\\\"")
          <> "\""
        None -> "NULL"
      }
    })
  value.Text("{" <> string.join(quoted, ",") <> "}")
}
"#;

const DECODE_ARRAY: &str = r#"
fn decode_array(
  inner: fn(Option(value.Value)) -> Result(a, error.Error),
) -> fn(Option(value.Value)) -> Result(List(a), error.Error) {
  fn(v) {
    case v {
      Some(value.Array(items)) -> decode_items(items, inner)
      // Arrays of enums and of other types without a binary codec arrive as
      // an array literal.
      Some(value.Text(literal)) -> {
        use items <- result.try(parse_array_literal(literal))
        decode_items(list.map(items, option.map(_, value.Text)), inner)
      }
      Some(_) -> Error(error.DecodeError("expected an array"))
      None -> Error(error.DecodeError("unexpected NULL array"))
    }
  }
}

fn decode_items(
  items: List(Option(value.Value)),
  inner: fn(Option(value.Value)) -> Result(a, error.Error),
) -> Result(List(a), error.Error) {
  list.try_map(items, fn(item) {
    case item, inner(item) {
      _, Ok(decoded) -> Ok(decoded)
      None, Error(_) ->
        Error(error.DecodeError(
          "the array holds a NULL; set the nullableArrayElements option of sqlc-gen-gleam to read such arrays",
        ))
      Some(_), Error(e) -> Error(e)
    }
  })
}

/// Parse a one-dimensional array literal such as `{a,"b c",NULL}`.
fn parse_array_literal(
  literal: String,
) -> Result(List(Option(String)), error.Error) {
  let bad = Error(error.DecodeError("expected an array, got " <> literal))
  case string.starts_with(literal, "{") && string.ends_with(literal, "}") {
    False -> bad
    True ->
      case string.slice(literal, 1, string.length(literal) - 2) {
        "" -> Ok([])
        body ->
          case array_items(string.to_graphemes(body), "", False, False, []) {
            Ok(items) -> Ok(items)
            Error(Nil) -> bad
          }
      }
  }
}

fn array_items(
  chars: List(String),
  item: String,
  quoted: Bool,
  in_quotes: Bool,
  acc: List(Option(String)),
) -> Result(List(Option(String)), Nil) {
  // An unquoted NULL is a missing element; a quoted one is the text "NULL".
  let finish = fn() {
    case quoted, string.uppercase(item) {
      False, "NULL" -> None
      _, _ -> Some(item)
    }
  }
  case chars, in_quotes {
    [], True -> Error(Nil)
    [], False -> Ok(list.reverse([finish(), ..acc]))
    ["\\", c, ..rest], _ -> array_items(rest, item <> c, quoted, in_quotes, acc)
    ["\"", ..rest], _ -> array_items(rest, item, True, !in_quotes, acc)
    [",", ..rest], False -> array_items(rest, "", False, False, [finish(), ..acc])
    // Nested arrays are not supported.
    ["{", ..], False -> Error(Nil)
    [c, ..rest], _ -> array_items(rest, item <> c, quoted, in_quotes, acc)
  }
}
"#;

const DECODE_NUMBER_AS_INT: &str = r#"
/// sqlc types sum() and similar aggregates as an integer where PostgreSQL
/// may answer with a numeric; a whole numeric is read as an Int.
fn decode_number_as_int(v: Option(value.Value)) -> Result(Int, error.Error) {
  case v {
    Some(value.Numeric(digits)) ->
      int.parse(digits)
      |> result.replace_error(error.DecodeError(
        "Expected Integer, got Numeric "
        <> digits
        <> "; override the column with dbType numeric in the sqlc-gen-gleam options",
      ))
    _ -> decode.int(v)
  }
}
"#;

const DECODE_NUMBER_AS_FLOAT: &str = r#"
/// sqlc types avg() and similar aggregates as a float where PostgreSQL may
/// answer with a numeric or an integer.
fn decode_number_as_float(v: Option(value.Value)) -> Result(Float, error.Error) {
  case v {
    Some(value.Integer(n)) -> Ok(int.to_float(n))
    Some(value.Numeric(digits)) ->
      case float.parse(digits), int.parse(digits) {
        Ok(f), _ -> Ok(f)
        _, Ok(n) -> Ok(int.to_float(n))
        _, _ ->
          Error(error.DecodeError("Expected Float, got Numeric " <> digits))
      }
    _ -> decode.float(v)
  }
}
"#;

const DECODE_AS_TEXT: &str = r#"
/// A type this plugin does not know is read as text, whichever way
/// PostgreSQL sends it (a domain arrives as the type it is built on).
fn decode_as_text(v: Option(value.Value)) -> Result(String, error.Error) {
  case v {
    Some(value.Integer(n)) | Some(value.Oid(n)) -> Ok(int.to_string(n))
    Some(value.Float(f)) -> Ok(float.to_string(f))
    Some(value.Numeric(digits)) -> Ok(digits)
    _ -> decode.text(v)
  }
}
"#;

const DECODE_OID: &str = r#"
fn decode_oid(v: Option(value.Value)) -> Result(Int, error.Error) {
  case v {
    Some(value.Oid(n)) -> Ok(n)
    _ -> decode.int(v)
  }
}
"#;

const INT_AS_TEXT: &str = r#"
/// A parameter of an aliased type (a domain) is sent as text: postgleam has
/// no codec for the domain's own type.
fn int_as_text(v: Int) -> value.Value {
  value.Text(int.to_string(v))
}
"#;

const FLOAT_AS_TEXT: &str = r#"
/// A parameter of an aliased type (a domain) is sent as text: postgleam has
/// no codec for the domain's own type.
fn float_as_text(v: Float) -> value.Value {
  value.Text(float.to_string(v))
}
"#;

const BOOL_AS_TEXT: &str = r#"
/// A parameter of an aliased type (a domain) is sent as text: postgleam has
/// no codec for the domain's own type.
fn bool_as_text(v: Bool) -> value.Value {
  case v {
    True -> value.Text("true")
    False -> value.Text("false")
  }
}
"#;

/// True if `body` calls the named private function.
fn uses_fn(body: &str, name: &str) -> bool {
    let needle = format!("{name}(");
    let bare = format!("{name})");
    body.contains(&needle) || body.contains(&bare) || body.contains(&format!("{name},"))
}

/// Map a PostgreSQL type name to Gleam type info, falling back to String for
/// a type this plugin does not know.
#[cfg(test)]
fn pg_type_to_gleam(pg_type: &str, uuid_as_string: bool) -> GleamType {
    pg_known_type(pg_type, uuid_as_string).unwrap_or_else(|| text_only("decode_as_text"))
}

/// Map a PostgreSQL type name to Gleam type info. `None` for a type this
/// plugin does not know.
fn pg_known_type(pg_type: &str, uuid_as_string: bool) -> Option<GleamType> {
    // Strip pg_catalog. prefix if present
    let type_name = pg_type
        .strip_prefix("pg_catalog.")
        .unwrap_or(pg_type)
        .to_lowercase();

    // postgleam uses value constructors (value.Text, value.Integer, etc.)
    // wrapped in Some() for pool queries that take List(Option(Value)).
    // The param_fn generates e.g. "Some(value.Text(val))" via the ParamExpr rendering.

    Some(match type_name.as_str() {
        // Boolean
        "bool" | "boolean" => GleamType::simple("Bool", "value.Boolean", "decode.bool"),

        // Integers
        "int2" | "smallint" | "smallserial" => {
            GleamType::simple("Int", "value.Integer", "decode.int")
        }
        "int4" | "integer" | "int" | "serial" => {
            GleamType::simple("Int", "value.Integer", "decode.int")
        }
        "int8" | "bigint" | "bigserial" => GleamType::simple("Int", "value.Integer", "decode.int"),

        // Floating point
        "float4" | "real" => GleamType::simple("Float", "value.Float", "decode.float"),
        "float8" | "double precision" | "double" => {
            GleamType::simple("Float", "value.Float", "decode.float")
        }

        // Numeric/Decimal
        "numeric" | "decimal" => GleamType::simple("String", "value.Numeric", "decode.numeric"),

        // Money (Int in PostGleam — cents as int64)
        "money" => GleamType::simple("Int", "value.Money", "decode.money"),

        // Text/String
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "name" => {
            GleamType::simple("String", "value.Text", "decode.text")
        }

        // Binary
        "bytea" => GleamType::simple("BitArray", "value.Bytea", "decode.bytea"),

        // UUID
        "uuid" => {
            if uuid_as_string {
                // Parsed with value.uuid_from_string before sending (see ParamExpr::UuidString).
                GleamType {
                    uuid_string: true,
                    ..GleamType::simple("String", "value.Uuid", "decode.uuid_string")
                }
            } else {
                GleamType::simple("BitArray", "value.Uuid", "decode.uuid")
            }
        }

        // JSON
        "json" => GleamType::simple("String", "value.Json", "decode.json"),
        "jsonb" => GleamType::simple("String", "value.Jsonb", "decode.jsonb"),

        // Date/Time — simple (arity 1)
        "date" => GleamType::simple("Int", "value.Date", "decode.date"),
        "timestamp" | "timestamp without time zone" => {
            GleamType::simple("Int", "value.Timestamp", "decode.timestamp")
        }
        "timestamptz" | "timestamp with time zone" => {
            GleamType::simple("Int", "value.Timestamptz", "decode.timestamptz")
        }
        "time" | "time without time zone" => GleamType::simple("Int", "value.Time", "decode.time"),

        // Date/Time — multi-arg
        "timetz" | "time with time zone" => {
            GleamType::multi("#(Int, Int)", "value.TimeTz", "decode.timetz", 2)
        }
        "interval" => GleamType::multi("#(Int, Int, Int)", "value.Interval", "decode.interval", 3),

        // XML
        "xml" => GleamType::simple("String", "value.Xml", "decode.xml"),

        // JSONPATH
        "jsonpath" => GleamType::simple("String", "value.Jsonpath", "decode.jsonpath"),

        // Geometric types — with param constructors
        "point" => GleamType::multi("#(Float, Float)", "value.Point", "decode.point", 2),
        "circle" => GleamType::multi("#(Float, Float, Float)", "value.Circle", "decode.circle", 3),

        // Geometric types
        "line" => GleamType::multi("#(Float, Float, Float)", "value.Line", "decode.line", 3),
        "lseg" => GleamType::multi(
            "#(Float, Float, Float, Float)",
            "value.Lseg",
            "decode.lseg",
            4,
        ),
        "box" => GleamType::multi(
            "#(Float, Float, Float, Float)",
            "value.Box",
            "decode.box",
            4,
        ),
        "path" => GleamType::multi(
            "#(Bool, List(#(Float, Float)))",
            "value.Path",
            "decode.path",
            2,
        ),
        "polygon" => GleamType::simple("List(#(Float, Float))", "value.Polygon", "decode.polygon"),

        // Network types
        "macaddr" => GleamType::simple("BitArray", "value.Macaddr", "decode.macaddr"),
        "macaddr8" => GleamType::simple("BitArray", "value.Macaddr8", "decode.macaddr8"),
        "cidr" | "inet" => {
            GleamType::multi("#(Int, BitArray, Int)", "value.Inet", "decode.inet", 3)
        }

        // Bit string
        "bit" | "varbit" | "bit varying" => GleamType::multi(
            "#(Int, BitArray)",
            "value.BitString",
            "decode.bit_string",
            2,
        ),

        // Full-text search (no binary codec support in PostGleam yet)
        "tsvector" | "tsquery" => text_only("decode.text"),

        // Object identifiers
        "oid" => GleamType::simple("Int", "value.Oid", "decode_oid"),

        // Void (for functions returning void)
        "void" => GleamType::simple("Nil", "fn(_) { value.Void }", "fn(_) { Ok(Nil) }"),

        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::type_map::EnumMap;

    #[test]
    fn test_basic_types() {
        let t = pg_type_to_gleam("bool", false);
        assert_eq!(t.type_name, "Bool");
        assert_eq!(t.param_fn, "value.Boolean");
        assert_eq!(t.decoder_fn, "decode.bool");
        assert_eq!(t.param_arity, 1);

        let t = pg_type_to_gleam("int4", false);
        assert_eq!(t.type_name, "Int");

        let t = pg_type_to_gleam("text", false);
        assert_eq!(t.type_name, "String");

        let t = pg_type_to_gleam("float8", false);
        assert_eq!(t.type_name, "Float");

        let t = pg_type_to_gleam("uuid", false);
        assert_eq!(t.type_name, "BitArray");

        let t = pg_type_to_gleam("jsonb", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "value.Jsonb");

        let t = pg_type_to_gleam("timestamptz", false);
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "value.Timestamptz");

        // sqlc pseudotype "any" falls back to String (unknown expressions)
        let t = pg_type_to_gleam("any", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "value.Text");
        assert_eq!(t.decoder_fn, "decode_as_text");
    }

    #[test]
    fn test_custom_module() {
        // Module param is unused — param_fn always uses value.Xxx constructors
        let t = pg_type_to_gleam("bool", false);
        assert_eq!(t.param_fn, "value.Boolean");
        assert_eq!(t.decoder_fn, "decode.bool");

        let t = pg_type_to_gleam("int4", false);
        assert_eq!(t.param_fn, "value.Integer");

        let t = pg_type_to_gleam("text", false);
        assert_eq!(t.param_fn, "value.Text");

        let t = pg_type_to_gleam("jsonb", false);
        assert_eq!(t.param_fn, "value.Jsonb");
    }

    #[test]
    fn test_pg_catalog_prefix() {
        let t = pg_type_to_gleam("pg_catalog.int4", false);
        assert_eq!(t.type_name, "Int");

        let t = pg_type_to_gleam("pg_catalog.bool", false);
        assert_eq!(t.type_name, "Bool");
    }

    #[test]
    fn test_unknown_type_fallback() {
        let t = pg_type_to_gleam("some_custom_type", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "value.Text");
    }

    #[test]
    fn test_time_types() {
        let t = pg_type_to_gleam("time", false);
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "value.Time");
        assert_eq!(t.decoder_fn, "decode.time");
        assert_eq!(t.param_arity, 1);

        let t = pg_type_to_gleam("timetz", false);
        assert_eq!(t.type_name, "#(Int, Int)");
        assert_eq!(t.param_fn, "value.TimeTz");
        assert_eq!(t.decoder_fn, "decode.timetz");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("interval", false);
        assert_eq!(t.type_name, "#(Int, Int, Int)");
        assert_eq!(t.param_fn, "value.Interval");
        assert_eq!(t.decoder_fn, "decode.interval");
        assert_eq!(t.param_arity, 3);
    }

    #[test]
    fn test_money_type() {
        let t = pg_type_to_gleam("money", false);
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "value.Money");
        assert_eq!(t.decoder_fn, "decode.money");
    }

    #[test]
    fn test_xml_jsonpath() {
        let t = pg_type_to_gleam("xml", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "value.Xml");
        assert_eq!(t.decoder_fn, "decode.xml");

        let t = pg_type_to_gleam("jsonpath", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "value.Jsonpath");
        assert_eq!(t.decoder_fn, "decode.jsonpath");
    }

    #[test]
    fn test_geometric_types() {
        let t = pg_type_to_gleam("point", false);
        assert_eq!(t.type_name, "#(Float, Float)");
        assert_eq!(t.param_fn, "value.Point");
        assert_eq!(t.decoder_fn, "decode.point");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("circle", false);
        assert_eq!(t.type_name, "#(Float, Float, Float)");
        assert_eq!(t.param_fn, "value.Circle");
        assert_eq!(t.decoder_fn, "decode.circle");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("line", false);
        assert_eq!(t.type_name, "#(Float, Float, Float)");
        assert_eq!(t.param_fn, "value.Line");
        assert_eq!(t.decoder_fn, "decode.line");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("lseg", false);
        assert_eq!(t.type_name, "#(Float, Float, Float, Float)");
        assert_eq!(t.param_fn, "value.Lseg");
        assert_eq!(t.decoder_fn, "decode.lseg");
        assert_eq!(t.param_arity, 4);

        let t = pg_type_to_gleam("box", false);
        assert_eq!(t.param_fn, "value.Box");
        assert_eq!(t.decoder_fn, "decode.box");
        assert_eq!(t.param_arity, 4);

        let t = pg_type_to_gleam("path", false);
        assert_eq!(t.type_name, "#(Bool, List(#(Float, Float)))");
        assert_eq!(t.param_fn, "value.Path");
        assert_eq!(t.decoder_fn, "decode.path");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("polygon", false);
        assert_eq!(t.type_name, "List(#(Float, Float))");
        assert_eq!(t.param_fn, "value.Polygon");
        assert_eq!(t.decoder_fn, "decode.polygon");
    }

    #[test]
    fn test_network_types() {
        let t = pg_type_to_gleam("macaddr", false);
        assert_eq!(t.type_name, "BitArray");
        assert_eq!(t.param_fn, "value.Macaddr");
        assert_eq!(t.decoder_fn, "decode.macaddr");

        let t = pg_type_to_gleam("macaddr8", false);
        assert_eq!(t.type_name, "BitArray");
        assert_eq!(t.param_fn, "value.Macaddr8");
        assert_eq!(t.decoder_fn, "decode.macaddr8");

        let t = pg_type_to_gleam("inet", false);
        assert_eq!(t.type_name, "#(Int, BitArray, Int)");
        assert_eq!(t.param_fn, "value.Inet");
        assert_eq!(t.decoder_fn, "decode.inet");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("cidr", false);
        assert_eq!(t.type_name, "#(Int, BitArray, Int)");
        assert_eq!(t.param_fn, "value.Inet");
        assert_eq!(t.decoder_fn, "decode.inet");
    }

    #[test]
    fn test_bit_string_type() {
        let t = pg_type_to_gleam("bit", false);
        assert_eq!(t.type_name, "#(Int, BitArray)");
        assert_eq!(t.param_fn, "value.BitString");
        assert_eq!(t.decoder_fn, "decode.bit_string");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("varbit", false);
        assert_eq!(t.type_name, "#(Int, BitArray)");
        assert_eq!(t.param_fn, "value.BitString");
        assert_eq!(t.decoder_fn, "decode.bit_string");
    }

    fn col(type_name: &str, not_null: bool, is_array: bool) -> Column {
        Column {
            not_null,
            is_array,
            r#type: Some(crate::plugin::plugin::Identifier {
                name: type_name.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_multi_arity_resolve() {
        let driver = PostgresDriver::new(&Options::default());
        let enums = EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);

        let resolved = driver.resolve_column_type(&col("point", true, false), &ctx);
        assert_eq!(resolved.type_expr, "#(Float, Float)");
        assert_eq!(
            driver.render_param(&resolved.param_expr, "p"),
            "Some(case p { #(a, b) -> value.Point(a, b) })"
        );

        let resolved = driver.resolve_column_type(&col("interval", false, false), &ctx);
        assert_eq!(resolved.type_expr, "Option(#(Int, Int, Int))");
        assert_eq!(
            driver.render_param(&resolved.param_expr, "i"),
            "option.map(i, fn(t) { case t { #(a, b, c) -> value.Interval(a, b, c) } })"
        );

        let resolved = driver.resolve_column_type(&col("int4", true, false), &ctx);
        assert_eq!(resolved.type_expr, "Int");
        assert_eq!(
            driver.render_param(&resolved.param_expr, "n"),
            "Some(value.Integer(n))"
        );
        let resolved = driver.resolve_column_type(&col("int4", false, false), &ctx);
        assert_eq!(
            driver.render_param(&resolved.param_expr, "n"),
            "option.map(n, value.Integer)"
        );
    }

    #[test]
    fn test_arrays() {
        let driver = PostgresDriver::new(&Options::default());
        let enums = EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        let resolved = driver.resolve_column_type(&col("int8", true, true), &ctx);
        assert_eq!(resolved.type_expr, "List(Int)");
        assert_eq!(resolved.decoder_expr.to_gleam(), "decode_array(decode.int)");
        assert_eq!(
            driver.render_param(&resolved.param_expr, "ids"),
            "Some(value.Array(list.map(ids, fn(e) { Some(value.Integer(e)) })))"
        );
        let resolved = driver.resolve_column_type(&col("text", false, true), &ctx);
        assert_eq!(resolved.type_expr, "Option(List(String))");
        assert_eq!(
            resolved.decoder_expr.to_gleam(),
            "decode.optional(decode_array(decode.text))"
        );
    }

    #[test]
    fn test_nullable_array_elements() {
        let driver = PostgresDriver::new(&Options {
            nullable_array_elements: true,
            uuid_as_string: true,
            ..Default::default()
        });
        let enums = EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        let resolved = driver.resolve_column_type(&col("int8", true, true), &ctx);
        assert_eq!(resolved.type_expr, "List(Option(Int))");
        assert_eq!(
            resolved.decoder_expr.to_gleam(),
            "decode_array(decode.optional(decode.int))"
        );
        assert_eq!(
            driver.render_param(&resolved.param_expr, "ids"),
            "Some(value.Array(list.map(ids, option.map(_, value.Integer))))"
        );
        // Scalars are not affected.
        let resolved = driver.resolve_column_type(&col("int8", true, false), &ctx);
        assert_eq!(resolved.type_expr, "Int");

        let resolved = driver.resolve_column_type(&col("uuid", true, true), &ctx);
        assert_eq!(resolved.type_expr, "List(Option(String))");
        let prepared = driver.prepare_param(&resolved.param_expr, "ids").unwrap();
        assert!(
            prepared.contains("option.None -> Ok(None)")
                && prepared.ends_with("Some(value.Array(vs)) })"),
            "{prepared}"
        );
    }

    #[test]
    fn test_function_results_are_read_leniently() {
        let driver = PostgresDriver::new(&Options::default());
        let enums = EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        let func = |type_name: &str| Column {
            is_func_call: true,
            ..col(type_name, true, false)
        };
        let decoder = |c: &Column| driver.resolve_column_type(c, &ctx).decoder_expr.to_gleam();
        assert_eq!(decoder(&func("bigint")), "decode_number_as_int");
        assert_eq!(decoder(&func("double precision")), "decode_number_as_float");
        assert_eq!(decoder(&func("text")), "decode.text");
        // A table column is read strictly.
        assert_eq!(decoder(&col("bigint", true, false)), "decode.int");
    }

    #[test]
    fn test_unknown_types_and_aliases() {
        let enums = EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);

        // An unknown type (a domain, say) is text both ways.
        let driver = PostgresDriver::new(&Options::default());
        assert!(!driver.knows_type("posint", &ctx));
        assert!(driver.knows_type("pg_catalog.int4", &ctx));
        let resolved = driver.resolve_column_type(&col("posint", true, false), &ctx);
        assert_eq!(resolved.type_expr, "String");
        assert_eq!(resolved.decoder_expr.to_gleam(), "decode_as_text");
        assert_eq!(
            driver.render_param(&resolved.param_expr, "n"),
            "Some(value.Text(n))"
        );
        // Its arrays travel as array literals.
        let resolved = driver.resolve_column_type(&col("posint", false, true), &ctx);
        assert_eq!(resolved.type_expr, "Option(List(String))");
        assert_eq!(
            resolved.decoder_expr.to_gleam(),
            "decode.optional(decode_array(decode_as_text))"
        );
        assert_eq!(
            driver.render_param(&resolved.param_expr, "ns"),
            "option.map(ns, fn(l) { array_literal(list.map(l, Some)) })"
        );

        // With an alias it has the type it is built on; parameters are sent
        // as text because postgleam has no codec for the domain itself.
        let driver = PostgresDriver::new(&Options {
            type_aliases: [("PosInt".to_string(), "integer".to_string())].into(),
            ..Default::default()
        });
        assert!(driver.knows_type("posint", &ctx));
        let resolved = driver.resolve_column_type(&col("posint", true, false), &ctx);
        assert_eq!(resolved.type_expr, "Int");
        assert_eq!(resolved.decoder_expr.to_gleam(), "decode.int");
        assert_eq!(
            driver.render_param(&resolved.param_expr, "n"),
            "Some(int_as_text(n))"
        );
        let resolved = driver.resolve_column_type(&col("posint", true, true), &ctx);
        assert_eq!(resolved.type_expr, "List(String)");

        let mut w = CodeWriter::new();
        driver.write_helpers(
            "[Some(int_as_text(n))]\ndecode.element(0, decode_as_text)\ndecode_array(decode.int)\narray_literal(x)",
            &ctx,
            &mut w,
        );
        let helpers = w.into_string();
        for name in [
            "fn int_as_text(",
            "fn decode_as_text(",
            "fn decode_array(",
            "fn parse_array_literal(",
            "fn array_literal(",
        ] {
            assert!(helpers.contains(name), "{name} missing in:\n{helpers}");
        }
        assert!(!helpers.contains("fn decode_oid("), "{helpers}");
    }

    #[test]
    fn test_oid_and_void() {
        let t = pg_type_to_gleam("oid", false);
        assert_eq!(
            (
                t.type_name.as_str(),
                t.param_fn.as_str(),
                t.decoder_fn.as_str()
            ),
            ("Int", "value.Oid", "decode_oid")
        );
        // value.Null does not exist in postgleam; value.Void does.
        let t = pg_type_to_gleam("void", false);
        assert_eq!(t.type_name, "Nil");
        assert_eq!(t.param_fn, "fn(_) { value.Void }");
    }

    #[test]
    fn test_custom_module_driver() {
        let options = Options {
            module: Some("db/pg".to_string()),
            ..Default::default()
        };
        let driver = PostgresDriver::new(&options);
        assert_eq!(driver.module_name(), "pg");
        assert_eq!(driver.import_path(), "db/pg");
        assert_eq!(driver.decode_import_path(), "db/pg/decode");
        let mut w = CodeWriter::new();
        driver.write_many_call("q", "[]", &mut w);
        assert_eq!(w.into_string(), "pg.query_with(conn, q, [], decoder)\n");
    }

    #[test]
    fn test_pool_module_defaults() {
        let options = Options {
            module: Some("postgleam/pool".to_string()),
            ..Default::default()
        };
        let driver = PostgresDriver::new(&options);
        assert_eq!(driver.decode_import_path(), "postgleam/decode");
        let mut w = CodeWriter::new();
        driver.write_many_call("q", "[]", &mut w);
        driver.write_one_call("q", "[]", &mut w);
        driver.write_copyfrom_call("q", "sets", &mut w);
        assert_eq!(
            w.into_string(),
            "pool.query_with(conn, q, [], decoder)\npool.query_one(conn, q, [], decoder)\npool.query_batch(conn, q, sets)\n|> result.map(list.length)\n"
        );
    }

    #[test]
    fn test_default_calls() {
        let driver = PostgresDriver::new(&Options::default());
        let mut w = CodeWriter::new();
        driver.write_one_call("q", "[]", &mut w);
        driver.write_exec_call("q", "[]", &mut w);
        driver.write_copyfrom_call("q", "sets", &mut w);
        assert_eq!(
            w.into_string(),
            "postgleam.query_one(conn, q, [], decoder)\npostgleam.query(conn, q, [])\n|> result.replace(Nil)\npostgleam.query_batch(conn, q, sets)\n|> result.map(list.length)\n"
        );
    }

    #[test]
    fn test_uuid_as_string() {
        let t = pg_type_to_gleam("uuid", true);
        assert_eq!(t.type_name, "String");
        assert!(t.uuid_string);
        assert_eq!(t.decoder_fn, "decode.uuid_string");

        // Default (false) should still be BitArray
        let t = pg_type_to_gleam("uuid", false);
        assert_eq!(t.type_name, "BitArray");
        assert_eq!(t.param_fn, "value.Uuid");

        // String UUID params are parsed, never sent as text or NULL.
        let driver = PostgresDriver::new(&Options {
            uuid_as_string: true,
            ..Default::default()
        });
        let enums = EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        let resolved = driver.resolve_column_type(&col("uuid", true, false), &ctx);
        assert_eq!(
            driver.prepare_param(&resolved.param_expr, "id").as_deref(),
            Some("uuid_param(id) |> result.map(Some)")
        );
        assert_eq!(
            driver.render_param(&resolved.param_expr, "id_param"),
            "id_param"
        );
        let resolved = driver.resolve_column_type(&col("uuid", false, false), &ctx);
        assert_eq!(
            driver.prepare_param(&resolved.param_expr, "id").as_deref(),
            Some(
                "case id { option.Some(u) -> uuid_param(u) |> result.map(Some)  option.None -> Ok(None) }"
            )
        );
    }

    #[test]
    fn test_enum_columns() {
        let driver = PostgresDriver::new(&Options::default());
        let mut enums = EnumMap::new();
        enums.insert(
            "status".into(),
            crate::driver::type_map::EnumInfo {
                type_name: "Status".into(),
                snake: "status".into(),
            },
        );
        let ctx = TypeContext {
            enums: &enums,
            enum_qualifier: Some("models.".into()),
        };
        let resolved = driver.resolve_column_type(&col("status", true, false), &ctx);
        assert_eq!(resolved.type_expr, "models.Status");
        assert_eq!(resolved.decoder_expr.to_gleam(), "decode_status");
        assert_eq!(
            driver.render_param(&resolved.param_expr, "s"),
            "Some(encode_status(s))"
        );
        // An enum array has no binary codec: it is an array literal.
        let resolved = driver.resolve_column_type(&col("status", true, true), &ctx);
        assert_eq!(resolved.type_expr, "List(models.Status)");
        assert_eq!(
            resolved.decoder_expr.to_gleam(),
            "decode_array(decode_status)"
        );
        assert_eq!(
            driver.render_param(&resolved.param_expr, "s"),
            "Some(array_literal(list.map(s, fn(e) { Some(models.status_to_string(e)) })))"
        );
        // Without a models module, enums are plain strings.
        let ctx = TypeContext::without_enums(&enums);
        let resolved = driver.resolve_column_type(&col("status", true, false), &ctx);
        assert_eq!(resolved.type_expr, "String");
        assert_eq!(resolved.decoder_expr.to_gleam(), "decode.text");
        let resolved = driver.resolve_column_type(&col("status", true, true), &ctx);
        assert_eq!(
            driver.render_param(&resolved.param_expr, "s"),
            "Some(array_literal(list.map(s, Some)))"
        );
    }
}
