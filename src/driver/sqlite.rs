use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::driver::traits::{Driver, path_alias, uses_module};
use crate::driver::type_map::{GleamType, ParamExpr, ResolvedType, Shape, TypeContext};
use crate::options::Options;
use crate::plugin::plugin::Column;
use crate::utils::CodeWriter;

/// Generates code for [glite](https://hex.pm/packages/glite).
pub struct SqliteDriver {
    /// Full import path, e.g., "glite" or "glite/tx"
    import: String,
    /// Module alias (last segment), e.g., "glite" or "tx"
    alias: String,
    /// Full decode import path
    decode_import: String,
    /// Module holding the param constructors (`int`, `text`, `nullable`, ...).
    /// `glite/tx` has none, so in that case they come from `glite`.
    value_module: String,
    /// User-declared aliases (uppercase) for types the plugin does not know.
    type_aliases: BTreeMap<String, String>,
    /// Unknown types already warned about, so each is reported once.
    warned: RefCell<BTreeSet<String>>,
}

impl SqliteDriver {
    pub fn new(options: &Options) -> Self {
        let import = options
            .module
            .clone()
            .unwrap_or_else(|| "glite".to_string());
        let alias = path_alias(&import);
        let decode_import = options.decode_module.clone().unwrap_or_else(|| {
            if import == "glite/tx" {
                "glite/decode".to_string()
            } else {
                format!("{import}/decode")
            }
        });
        let value_module = if import == "glite/tx" {
            "glite".to_string()
        } else {
            alias.clone()
        };
        Self {
            import,
            alias,
            decode_import,
            value_module,
            type_aliases: options
                .type_aliases
                .iter()
                .map(|(k, v)| (normalize(k), v.clone()))
                .collect(),
            warned: RefCell::new(BTreeSet::new()),
        }
    }
}

/// A declared type in the form the type tables use: uppercase, without a
/// length or precision such as the `(10,2)` of `DECIMAL(10,2)`.
fn normalize(sqlite_type: &str) -> String {
    let upper = sqlite_type.to_uppercase();
    upper.split('(').next().unwrap_or("").trim().to_string()
}

impl Driver for SqliteDriver {
    fn module_name(&self) -> &str {
        &self.alias
    }

    fn import_path(&self) -> &str {
        &self.import
    }

    fn decode_import_path(&self) -> &str {
        &self.decode_import
    }

    fn resolve_column_type(&self, col: &Column, _ctx: &TypeContext) -> ResolvedType {
        let col_type = col
            .r#type
            .as_ref()
            .map(|t| t.name.as_str())
            .unwrap_or("text");
        let declared = normalize(col_type);
        let declared = self
            .type_aliases
            .get(&declared)
            .map(|target| normalize(target))
            .unwrap_or(declared);
        let mut base = sqlite_known_type(&declared, &self.value_module).unwrap_or_else(|| {
            if self.warned.borrow_mut().insert(declared.clone()) {
                eprintln!(
                    "warning: unknown SQLite type '{col_type}' (e.g. column '{}'): typed as String. \
                     If that is wrong, name a type the plugin knows with the typeAliases option, \
                     or override the column.",
                    col.name
                );
            }
            text_or_number(&self.value_module)
        });
        // Only a REAL column is sure to hold a float: an expression such as
        // sum() or max() answers with an integer when the number is whole.
        let from_table = col.table.as_ref().is_some_and(|t| !t.name.is_empty());
        if base.decoder_fn == "decode.float" && !from_table {
            base.decoder_fn = "decode_number".into();
        }
        // SQLite has no array types.
        ResolvedType::from_base(base, Shape::scalar(!col.not_null), None)
    }

    fn knows_type(&self, type_name: &str, _ctx: &TypeContext) -> bool {
        let declared = normalize(type_name);
        self.type_aliases.contains_key(&declared)
            || sqlite_known_type(&declared, &self.value_module).is_some()
    }

    fn override_base(&self, type_name: &str) -> GleamType {
        let m = &self.value_module;
        match type_name {
            "String" => GleamType::simple("String", &format!("{m}.text"), "decode.text"),
            "Int" => GleamType::simple("Int", &format!("{m}.int"), "decode.int"),
            // An overridden column is usually an expression, which answers
            // with an integer when the number is whole.
            "Float" => GleamType::simple("Float", &format!("{m}.float"), "decode_number"),
            "Bool" => GleamType::simple("Bool", &format!("{m}.bool"), "decode.bool"),
            "BitArray" => GleamType::simple("BitArray", &format!("{m}.blob"), "decode.blob"),
            other => {
                unreachable!("override type '{other}' should have been rejected by Options::parse")
            }
        }
    }

    fn render_param(&self, expr: &ParamExpr, var: &str) -> String {
        let m = &self.value_module;
        match expr {
            ParamExpr::Value {
                fn_name,
                arity: 1,
                nullable: false,
            } => format!("{fn_name}({var})"),
            ParamExpr::Value {
                fn_name,
                arity: 1,
                nullable: true,
            } => {
                format!("{m}.nullable({var}, {fn_name})")
            }
            other => unreachable!("SQLite driver never produces {other:?}"),
        }
    }

    fn render_slice_param(&self, expr: &ParamExpr, var: &str) -> String {
        let m = &self.value_module;
        match expr {
            ParamExpr::Value {
                fn_name,
                arity: 1,
                nullable: false,
            } => format!("list.map({var}, {fn_name})"),
            ParamExpr::Value {
                fn_name,
                arity: 1,
                nullable: true,
            } => {
                format!("list.map({var}, fn(v) {{ {m}.nullable(v, {fn_name}) }})")
            }
            other => unreachable!("SQLite driver never produces {other:?}"),
        }
    }

    fn extra_imports(&self, body: &str) -> Vec<String> {
        let mut imports = Vec::new();
        if self.value_module != self.alias && uses_module(body, &self.value_module) {
            imports.push(self.value_module.clone());
        }
        // The helpers below match on glite's value type.
        if uses_module(body, "value") {
            imports.push("glite/value".to_string());
        }
        imports
    }

    fn write_helpers(&self, body: &str, _ctx: &TypeContext, w: &mut CodeWriter) {
        let mut wrote = false;
        for (name, text) in [
            ("decode_number", DECODE_NUMBER),
            ("decode_text_or_number", DECODE_TEXT_OR_NUMBER),
        ] {
            let used = [format!("{name})"), format!("{name},")]
                .iter()
                .any(|needle| body.contains(needle));
            if used {
                if wrote {
                    w.blank();
                }
                wrote = true;
                for line in text.trim_matches('\n').lines() {
                    w.line(line);
                }
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
        w.writef(format_args!("{m}.query(conn, {sql}, {params}, decoder)"));
    }

    fn write_exec_call(&self, sql: &str, params: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        w.writef(format_args!("{m}.exec(conn, {sql}, {params})"));
        w.line("|> result.replace(Nil)");
    }

    fn write_execrows_call(&self, sql: &str, params: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        // exec returns the number of rows changed.
        w.writef(format_args!("{m}.exec(conn, {sql}, {params})"));
    }

    fn write_copyfrom_call(&self, sql: &str, param_sets: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        // exec_many is all or nothing and returns the rows changed in total.
        w.writef(format_args!("{m}.exec_many(conn, {sql}, {param_sets})"));
    }
}

const DECODE_NUMBER: &str = r#"
/// SQLite stores a whole number as an integer, in a NUMERIC column and as
/// the result of an expression such as sum().
fn decode_number(v: value.Value) -> Result(Float, String) {
  case v {
    value.Integer(n) -> Ok(int.to_float(n))
    _ -> decode.float(v)
  }
}
"#;

const DECODE_TEXT_OR_NUMBER: &str = r#"
/// Date, time and untyped columns have NUMERIC affinity: SQLite stores text
/// that looks like a number as a number. It is read back as text.
fn decode_text_or_number(v: value.Value) -> Result(String, String) {
  case v {
    value.Integer(n) -> Ok(int.to_string(n))
    value.Real(f) -> Ok(float.to_string(f))
    _ -> decode.text(v)
  }
}
"#;

/// A String column with NUMERIC affinity, which may hold a number.
fn text_or_number(module: &str) -> GleamType {
    GleamType::simple("String", &format!("{module}.text"), "decode_text_or_number")
}

/// Map a SQLite type name to Gleam type info, falling back to String for a
/// type none of the rules cover.
#[cfg(test)]
fn sqlite_type_to_gleam(sqlite_type: &str, module: &str) -> GleamType {
    sqlite_known_type(&normalize(sqlite_type), module).unwrap_or_else(|| text_or_number(module))
}

/// Map a declared SQLite type (see `normalize`) to Gleam type info. `None`
/// for a type none of the rules cover.
///
/// SQLite uses type affinity rules. The declared type is mapped to one of
/// five affinities: INTEGER, REAL, TEXT, BLOB, or NUMERIC.
/// See: https://www.sqlite.org/datatype3.html
fn sqlite_known_type(type_name: &str, module: &str) -> Option<GleamType> {
    let int = || GleamType::simple("Int", &format!("{module}.int"), "decode.int");
    let float = || GleamType::simple("Float", &format!("{module}.float"), "decode.float");
    let text = || GleamType::simple("String", &format!("{module}.text"), "decode.text");
    let blob = || GleamType::simple("BitArray", &format!("{module}.blob"), "decode.blob");

    Some(match type_name {
        // Integer affinity
        "INT" | "INTEGER" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT"
        | "UNSIGNED BIG INT" | "INT2" | "INT8" => int(),

        // Boolean (stored as INTEGER 0/1 in SQLite)
        "BOOLEAN" | "BOOL" => GleamType::simple("Bool", &format!("{module}.bool"), "decode.bool"),

        // Real affinity
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" => float(),

        // Text affinity
        "TEXT" | "CHARACTER" | "VARCHAR" | "VARYING CHARACTER" | "NCHAR" | "NATIVE CHARACTER"
        | "NVARCHAR" | "CLOB" => text(),

        // Blob affinity
        "BLOB" => blob(),

        // Numeric affinity: SQLite stores these as INTEGER or REAL, never as
        // an exact decimal, so they are floats.
        "NUMERIC" | "DECIMAL" => {
            GleamType::simple("Float", &format!("{module}.float"), "decode_number")
        }

        // Date and time types also have NUMERIC affinity. They usually hold
        // ISO-8601 text; a unix time or Julian day comes back as its digits.
        "DATE" | "DATETIME" | "TIMESTAMP" => text_or_number(module),

        // What sqlc calls a column declared without a type, and an expression
        // it cannot type: any storage class can turn up.
        "ANY" | "" => text_or_number(module),

        // SQLite's affinity rules for every other declared type:
        // 1. Contains "INT" → INTEGER affinity
        // 2. Contains "CHAR", "CLOB", or "TEXT" → TEXT affinity
        // 3. Contains "BLOB" → BLOB affinity
        // 4. Contains "REAL", "FLOA", or "DOUB" → REAL affinity
        // 5. Otherwise → NUMERIC affinity, reported as unknown
        _ => {
            if type_name.contains("INT") {
                int()
            } else if type_name.contains("CHAR")
                || type_name.contains("CLOB")
                || type_name.contains("TEXT")
            {
                text()
            } else if type_name.contains("BLOB") {
                blob()
            } else if type_name.contains("REAL")
                || type_name.contains("FLOA")
                || type_name.contains("DOUB")
            {
                float()
            } else {
                return None;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_types() {
        for t in &[
            "INTEGER",
            "INT",
            "TINYINT",
            "SMALLINT",
            "MEDIUMINT",
            "BIGINT",
            "INT2",
            "INT8",
        ] {
            let g = sqlite_type_to_gleam(t, "glite");
            assert_eq!(g.type_name, "Int", "failed for type {t}");
            assert_eq!(g.param_fn, "glite.int");
            assert_eq!(g.decoder_fn, "decode.int");
        }
    }

    #[test]
    fn test_boolean() {
        let g = sqlite_type_to_gleam("BOOLEAN", "glite");
        assert_eq!(g.type_name, "Bool");
        assert_eq!(g.param_fn, "glite.bool");
        assert_eq!(g.decoder_fn, "decode.bool");
    }

    #[test]
    fn test_real_types() {
        for t in &["REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT"] {
            let g = sqlite_type_to_gleam(t, "glite");
            assert_eq!(g.type_name, "Float", "failed for type {t}");
            assert_eq!(g.param_fn, "glite.float");
        }
    }

    #[test]
    fn test_text_types() {
        for t in &["TEXT", "CHARACTER", "VARCHAR", "NCHAR", "NVARCHAR", "CLOB"] {
            let g = sqlite_type_to_gleam(t, "glite");
            assert_eq!(g.type_name, "String", "failed for type {t}");
            assert_eq!(g.param_fn, "glite.text");
        }
    }

    #[test]
    fn test_blob() {
        let g = sqlite_type_to_gleam("BLOB", "glite");
        assert_eq!(g.type_name, "BitArray");
        assert_eq!(g.param_fn, "glite.blob");
        assert_eq!(g.decoder_fn, "decode.blob");
    }

    #[test]
    fn test_datetime_as_text() {
        // NUMERIC affinity: text that looks like a number is stored as one,
        // so the decoder accepts numbers too.
        for t in &["DATE", "DATETIME", "TIMESTAMP", "ANY", "WIDGET"] {
            let g = sqlite_type_to_gleam(t, "glite");
            assert_eq!(g.type_name, "String", "failed for type {t}");
            assert_eq!(g.param_fn, "glite.text");
            assert_eq!(g.decoder_fn, "decode_text_or_number");
        }
    }

    #[test]
    fn test_numeric_and_decimal_are_floats() {
        for t in &["NUMERIC", "DECIMAL", "decimal(10, 2)", "NUMERIC(8)"] {
            let g = sqlite_type_to_gleam(t, "glite");
            assert_eq!(g.type_name, "Float", "failed for type {t}");
            assert_eq!(g.param_fn, "glite.float");
            assert_eq!(g.decoder_fn, "decode_number");
        }
        // A length does not change the type.
        assert_eq!(
            sqlite_type_to_gleam("VARCHAR(20)", "glite").type_name,
            "String"
        );
    }

    #[test]
    fn test_expressions_and_aliases() {
        let enums = crate::driver::type_map::EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        let column = |type_name: &str, table: &str| Column {
            not_null: true,
            r#type: Some(crate::plugin::plugin::Identifier {
                name: type_name.into(),
                ..Default::default()
            }),
            table: Some(crate::plugin::plugin::Identifier {
                name: table.into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let driver = SqliteDriver::new(&Options::default());
        let decoder =
            |d: &SqliteDriver, c: &Column| d.resolve_column_type(c, &ctx).decoder_expr.to_gleam();
        // A REAL column always holds a float; sum() may answer an integer.
        assert_eq!(decoder(&driver, &column("REAL", "t")), "decode.float");
        assert_eq!(decoder(&driver, &column("real", "")), "decode_number");
        assert!(driver.knows_type("DECIMAL(10,2)", &ctx));
        assert!(!driver.knows_type("MONEY", &ctx));

        let driver = SqliteDriver::new(&Options {
            type_aliases: [("money".to_string(), "INTEGER".to_string())].into(),
            ..Default::default()
        });
        assert!(driver.knows_type("MONEY", &ctx));
        assert_eq!(decoder(&driver, &column("MONEY", "t")), "decode.int");

        let mut w = CodeWriter::new();
        driver.write_helpers("decode.element(0, decode_number)", &ctx, &mut w);
        let helpers = w.into_string();
        assert!(helpers.contains("fn decode_number("), "{helpers}");
        assert!(!helpers.contains("fn decode_text_or_number("), "{helpers}");
        assert_eq!(
            driver.extra_imports(&helpers),
            vec!["glite/value".to_string()]
        );
    }

    #[test]
    fn test_affinity_rules() {
        // Contains INT → integer affinity
        let g = sqlite_type_to_gleam("UNSIGNED BIG INT", "glite");
        assert_eq!(g.type_name, "Int");

        // Contains CHAR → text affinity
        let g = sqlite_type_to_gleam("VARYING CHARACTER(255)", "glite");
        assert_eq!(g.type_name, "String");

        // Contains REAL → real affinity
        let g = sqlite_type_to_gleam("SOME REAL TYPE", "glite");
        assert_eq!(g.type_name, "Float");
    }

    #[test]
    fn test_case_insensitive() {
        let g = sqlite_type_to_gleam("integer", "glite");
        assert_eq!(g.type_name, "Int");

        let g = sqlite_type_to_gleam("Text", "glite");
        assert_eq!(g.type_name, "String");
    }

    #[test]
    fn test_custom_module() {
        let g = sqlite_type_to_gleam("INTEGER", "db");
        assert_eq!(g.param_fn, "db.int");
        assert_eq!(g.decoder_fn, "decode.int");

        let g = sqlite_type_to_gleam("TEXT", "db");
        assert_eq!(g.param_fn, "db.text");
    }

    #[test]
    fn test_tx_module_uses_glite_constructors() {
        let options = Options {
            module: Some("glite/tx".into()),
            ..Default::default()
        };
        let driver = SqliteDriver::new(&options);
        assert_eq!(driver.decode_import_path(), "glite/decode");
        let enums = crate::driver::type_map::EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        let col = Column {
            not_null: false,
            r#type: Some(crate::plugin::plugin::Identifier {
                name: "integer".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = driver.resolve_column_type(&col, &ctx);
        assert_eq!(
            driver.render_param(&resolved.param_expr, "n"),
            "glite.nullable(n, glite.int)"
        );
        assert_eq!(
            driver.extra_imports("  tx.exec(conn, q, [glite.int(1)])"),
            vec!["glite".to_string()]
        );
        let mut w = CodeWriter::new();
        driver.write_many_call("q", "[]", &mut w);
        assert_eq!(w.into_string(), "tx.query(conn, q, [], decoder)\n");
    }
}
