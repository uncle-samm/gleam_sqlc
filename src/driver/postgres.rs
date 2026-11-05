use crate::driver::traits::{Driver, path_alias, uses_module};
use crate::driver::type_map::{
    GleamType, ParamExpr, ResolvedType, TypeContext, tuple_pattern, value_fn,
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
        }
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

fn array_value(elem_fn: &str, arity: u8, var: &str) -> String {
    format!(
        "value.Array(list.map({var}, fn(e) {{ Some({}) }}))",
        call_value(elem_fn, arity, "e")
    )
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
        let type_id = col.r#type.as_ref();
        let type_name = type_id.map(|t| t.name.as_str()).unwrap_or("text");
        let schema = type_id.map(|t| t.schema.as_str()).unwrap_or("");

        let enum_info = ctx.enum_qualifier.as_ref().and_then(|q| {
            let key = if schema.is_empty() || schema == "public" {
                type_name.to_string()
            } else {
                format!("{schema}.{type_name}")
            };
            ctx.enums.get(&key).map(|info| (q, info))
        });

        let base = match enum_info {
            Some((q, info)) => GleamType::simple(
                &format!("{q}{}", info.type_name),
                &format!("encode_{}", info.snake),
                &format!("decode_{}", info.snake),
            ),
            None => pg_type_to_gleam(type_name, self.uuid_as_string),
        };
        let array_decoder = format!("decode_array({})", base.decoder_fn);
        ResolvedType::from_base(base, !col.not_null, col.is_array, Some(array_decoder))
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
                nullable: false,
            } => {
                format!("Some({})", array_value(elem_fn, *arity, var))
            }
            ParamExpr::Array {
                elem_fn,
                arity,
                nullable: true,
            } => {
                format!(
                    "option.map({var}, fn(l) {{ {} }})",
                    array_value(elem_fn, *arity, "l")
                )
            }
            // Prepared by `prepare_param`; `var` is already the bound Param.
            ParamExpr::UuidString { .. } => var.to_string(),
        }
    }

    fn prepare_param(&self, expr: &ParamExpr, var: &str) -> Option<String> {
        let ParamExpr::UuidString { nullable, array } = expr else {
            return None;
        };
        let single = |v: &str| format!("uuid_param({v}) |> result.map(Some)");
        let many = |v: &str| {
            format!(
                "list.try_map({v}, uuid_param) |> result.map(fn(vs) {{ Some(value.Array(list.map(vs, Some))) }})"
            )
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
        if body.contains("decode_array(") {
            sep(w);
            w.line("fn decode_array(");
            w.line("  inner: fn(Option(value.Value)) -> Result(a, error.Error),");
            w.line(") -> fn(Option(value.Value)) -> Result(List(a), error.Error) {");
            w.line("  fn(v) {");
            w.line("    case v {");
            w.line("      Some(value.Array(items)) -> list.try_map(items, inner)");
            w.line("      Some(_) -> Error(error.DecodeError(\"expected an array\"))");
            w.line("      None -> Error(error.DecodeError(\"unexpected NULL array\"))");
            w.line("    }");
            w.line("  }");
            w.line("}");
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

/// True if `body` calls the named private function.
fn uses_fn(body: &str, name: &str) -> bool {
    let needle = format!("{name}(");
    let bare = format!("{name})");
    body.contains(&needle) || body.contains(&bare) || body.contains(&format!("{name},"))
}

/// Map a PostgreSQL type name to Gleam type info.
/// The `module` parameter is the module alias used in param constructors
/// (e.g., "postgleam" or "pg").
fn pg_type_to_gleam(pg_type: &str, uuid_as_string: bool) -> GleamType {
    // Strip pg_catalog. prefix if present
    let type_name = pg_type
        .strip_prefix("pg_catalog.")
        .unwrap_or(pg_type)
        .to_lowercase();

    // postgleam uses value constructors (value.Text, value.Integer, etc.)
    // wrapped in Some() for pool queries that take List(Option(Value)).
    // The param_fn generates e.g. "Some(value.Text(val))" via the ParamExpr rendering.

    match type_name.as_str() {
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
        "tsvector" | "tsquery" => GleamType::simple("String", "value.Text", "decode.text"),

        // Void (for functions returning void)
        "void" => GleamType::simple("Nil", "fn(_) { value.Null }", "fn(_) { Ok(Nil) }"),

        // Unknown — fall back to String
        _ => {
            eprintln!("warning: unknown PostgreSQL type '{pg_type}', falling back to String");
            GleamType::simple("String", "value.Text", "decode.text")
        }
    }
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
        assert_eq!(t.decoder_fn, "decode.text");
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
        // Without a models module, enums are plain strings.
        let ctx = TypeContext::without_enums(&enums);
        let resolved = driver.resolve_column_type(&col("status", true, false), &ctx);
        assert_eq!(resolved.type_expr, "String");
    }
}
