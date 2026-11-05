use crate::driver::traits::{Driver, path_alias, uses_module};
use crate::driver::type_map::{GleamType, ParamExpr, ResolvedType, TypeContext};
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
        }
    }
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
        let base = sqlite_type_to_gleam(col_type, &self.value_module);
        // SQLite has no array types.
        ResolvedType::from_base(base, !col.not_null, false, None)
    }

    fn override_base(&self, type_name: &str) -> GleamType {
        let m = &self.value_module;
        match type_name {
            "String" => GleamType::simple("String", &format!("{m}.text"), "decode.text"),
            "Int" => GleamType::simple("Int", &format!("{m}.int"), "decode.int"),
            "Float" => GleamType::simple("Float", &format!("{m}.float"), "decode.float"),
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
        if self.value_module != self.alias && uses_module(body, &self.value_module) {
            vec![self.value_module.clone()]
        } else {
            Vec::new()
        }
    }

    fn write_helpers(&self, _body: &str, _ctx: &TypeContext, _w: &mut CodeWriter) {}

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

/// Map a SQLite type name to Gleam type info.
///
/// SQLite uses type affinity rules. The declared type is mapped to one of
/// five storage classes: INTEGER, REAL, TEXT, BLOB, or NUMERIC.
/// See: https://www.sqlite.org/datatype3.html
fn sqlite_type_to_gleam(sqlite_type: &str, module: &str) -> GleamType {
    let type_name = sqlite_type.to_uppercase();
    let type_name = type_name.trim();

    // SQLite type affinity rules (simplified for common types)
    match type_name {
        // Integer affinity
        "INT" | "INTEGER" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT"
        | "UNSIGNED BIG INT" | "INT2" | "INT8" => {
            GleamType::simple("Int", &format!("{module}.int"), "decode.int")
        }

        // Boolean (stored as INTEGER 0/1 in SQLite)
        "BOOLEAN" | "BOOL" => GleamType::simple("Bool", &format!("{module}.bool"), "decode.bool"),

        // Real affinity
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" => {
            GleamType::simple("Float", &format!("{module}.float"), "decode.float")
        }

        // Text affinity
        "TEXT" | "CHARACTER" | "VARCHAR" | "VARYING CHARACTER" | "NCHAR" | "NATIVE CHARACTER"
        | "NVARCHAR" | "CLOB" => {
            GleamType::simple("String", &format!("{module}.text"), "decode.text")
        }

        // Blob affinity
        "BLOB" => GleamType::simple("BitArray", &format!("{module}.blob"), "decode.blob"),

        // Numeric affinity (decimal/numeric → String since Gleam has no Decimal)
        "NUMERIC" | "DECIMAL" => {
            GleamType::simple("String", &format!("{module}.text"), "decode.text")
        }

        // Date/time types (stored as TEXT in SQLite, or INTEGER for unix epoch)
        "DATE" | "DATETIME" | "TIMESTAMP" => {
            GleamType::simple("String", &format!("{module}.text"), "decode.text")
        }

        // Apply SQLite type affinity rules for unrecognized types:
        // 1. Contains "INT" → INTEGER affinity
        // 2. Contains "CHAR", "CLOB", or "TEXT" → TEXT affinity
        // 3. Contains "BLOB" or empty → BLOB affinity
        // 4. Contains "REAL", "FLOA", or "DOUB" → REAL affinity
        // 5. Otherwise → NUMERIC affinity (treat as TEXT)
        _ => {
            if type_name.contains("INT") {
                GleamType::simple("Int", &format!("{module}.int"), "decode.int")
            } else if type_name.contains("CHAR")
                || type_name.contains("CLOB")
                || type_name.contains("TEXT")
            {
                GleamType::simple("String", &format!("{module}.text"), "decode.text")
            } else if type_name.contains("BLOB") || type_name.is_empty() {
                GleamType::simple("BitArray", &format!("{module}.blob"), "decode.blob")
            } else if type_name.contains("REAL")
                || type_name.contains("FLOA")
                || type_name.contains("DOUB")
            {
                GleamType::simple("Float", &format!("{module}.float"), "decode.float")
            } else {
                eprintln!("warning: unknown SQLite type '{sqlite_type}', falling back to String");
                GleamType::simple("String", &format!("{module}.text"), "decode.text")
            }
        }
    }
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
        for t in &["DATE", "DATETIME", "TIMESTAMP"] {
            let g = sqlite_type_to_gleam(t, "glite");
            assert_eq!(g.type_name, "String", "failed for type {t}");
        }
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
