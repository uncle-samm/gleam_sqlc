use crate::driver::traits::Driver;
use crate::driver::type_map::{DecoderExpr, GleamType, ParamExpr, ResolvedType};
use crate::plugin::plugin::Column;
use crate::utils::CodeWriter;

pub struct SqliteDriver;

impl Driver for SqliteDriver {
    fn module_name(&self) -> &str {
        "glite"
    }

    fn resolve_column_type(&self, col: &Column) -> ResolvedType {
        let col_type = col
            .r#type
            .as_ref()
            .map(|t| t.name.as_str())
            .unwrap_or("text");
        let base = sqlite_type_to_gleam(col_type);
        let is_nullable = !col.not_null;

        // SQLite doesn't have native array types
        if is_nullable {
            ResolvedType {
                type_expr: format!("Option({})", base.type_name),
                param_expr: ParamExpr::Nullable {
                    inner_fn: base.param_fn,
                },
                decoder_expr: DecoderExpr::Optional {
                    inner_fn: base.decoder_fn,
                },
            }
        } else {
            ResolvedType {
                type_expr: base.type_name,
                param_expr: ParamExpr::Direct {
                    fn_name: base.param_fn,
                },
                decoder_expr: DecoderExpr::Direct {
                    fn_name: base.decoder_fn,
                },
            }
        }
    }

    fn execrows_needs_tag_parse(&self) -> bool {
        false
    }

    fn write_exec_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter) {
        // glite.exec returns Result(Int, Error) — the Int is discarded for :exec
        w.writef(format_args!(
            "glite.exec(conn, {const_name}, {params_str})"
        ));
    }

    fn write_execrows_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter) {
        // glite.exec returns Result(Int, Error) — the Int IS the affected row count
        w.writef(format_args!(
            "glite.exec(conn, {const_name}, {params_str})"
        ));
    }
}

/// Map a SQLite type name to Gleam type info.
///
/// SQLite uses type affinity rules. The declared type is mapped to one of
/// five storage classes: INTEGER, REAL, TEXT, BLOB, or NUMERIC.
/// See: https://www.sqlite.org/datatype3.html
fn sqlite_type_to_gleam(sqlite_type: &str) -> GleamType {
    let type_name = sqlite_type.to_uppercase();
    let type_name = type_name.trim();

    // SQLite type affinity rules (simplified for common types)
    match type_name {
        // Integer affinity
        "INT" | "INTEGER" | "TINYINT" | "SMALLINT" | "MEDIUMINT" | "BIGINT"
        | "UNSIGNED BIG INT" | "INT2" | "INT8" => {
            GleamType::simple("Int", "glite.int", "decode.int")
        }

        // Boolean (stored as INTEGER 0/1 in SQLite)
        "BOOLEAN" | "BOOL" => GleamType::simple("Bool", "glite.bool", "decode.bool"),

        // Real affinity
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" => {
            GleamType::simple("Float", "glite.float", "decode.float")
        }

        // Text affinity
        "TEXT" | "CHARACTER" | "VARCHAR" | "VARYING CHARACTER" | "NCHAR"
        | "NATIVE CHARACTER" | "NVARCHAR" | "CLOB" => {
            GleamType::simple("String", "glite.text", "decode.text")
        }

        // Blob affinity
        "BLOB" => GleamType::simple("BitArray", "glite.blob", "decode.blob"),

        // Numeric affinity (decimal/numeric → String since Gleam has no Decimal)
        "NUMERIC" | "DECIMAL" => GleamType::simple("String", "glite.text", "decode.text"),

        // Date/time types (stored as TEXT in SQLite, or INTEGER for unix epoch)
        "DATE" | "DATETIME" | "TIMESTAMP" => {
            GleamType::simple("String", "glite.text", "decode.text")
        }

        // Apply SQLite type affinity rules for unrecognized types:
        // 1. Contains "INT" → INTEGER affinity
        // 2. Contains "CHAR", "CLOB", or "TEXT" → TEXT affinity
        // 3. Contains "BLOB" or empty → BLOB affinity
        // 4. Contains "REAL", "FLOA", or "DOUB" → REAL affinity
        // 5. Otherwise → NUMERIC affinity (treat as TEXT)
        _ => {
            if type_name.contains("INT") {
                GleamType::simple("Int", "glite.int", "decode.int")
            } else if type_name.contains("CHAR")
                || type_name.contains("CLOB")
                || type_name.contains("TEXT")
            {
                GleamType::simple("String", "glite.text", "decode.text")
            } else if type_name.contains("BLOB") || type_name.is_empty() {
                GleamType::simple("BitArray", "glite.blob", "decode.blob")
            } else if type_name.contains("REAL")
                || type_name.contains("FLOA")
                || type_name.contains("DOUB")
            {
                GleamType::simple("Float", "glite.float", "decode.float")
            } else {
                eprintln!(
                    "warning: unknown SQLite type '{sqlite_type}', falling back to String"
                );
                GleamType::simple("String", "glite.text", "decode.text")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_types() {
        for t in &["INTEGER", "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT", "INT2", "INT8"]
        {
            let g = sqlite_type_to_gleam(t);
            assert_eq!(g.type_name, "Int", "failed for type {t}");
            assert_eq!(g.param_fn, "glite.int");
            assert_eq!(g.decoder_fn, "decode.int");
        }
    }

    #[test]
    fn test_boolean() {
        let g = sqlite_type_to_gleam("BOOLEAN");
        assert_eq!(g.type_name, "Bool");
        assert_eq!(g.param_fn, "glite.bool");
        assert_eq!(g.decoder_fn, "decode.bool");
    }

    #[test]
    fn test_real_types() {
        for t in &["REAL", "DOUBLE", "DOUBLE PRECISION", "FLOAT"] {
            let g = sqlite_type_to_gleam(t);
            assert_eq!(g.type_name, "Float", "failed for type {t}");
            assert_eq!(g.param_fn, "glite.float");
        }
    }

    #[test]
    fn test_text_types() {
        for t in &["TEXT", "CHARACTER", "VARCHAR", "NCHAR", "NVARCHAR", "CLOB"] {
            let g = sqlite_type_to_gleam(t);
            assert_eq!(g.type_name, "String", "failed for type {t}");
            assert_eq!(g.param_fn, "glite.text");
        }
    }

    #[test]
    fn test_blob() {
        let g = sqlite_type_to_gleam("BLOB");
        assert_eq!(g.type_name, "BitArray");
        assert_eq!(g.param_fn, "glite.blob");
        assert_eq!(g.decoder_fn, "decode.blob");
    }

    #[test]
    fn test_datetime_as_text() {
        for t in &["DATE", "DATETIME", "TIMESTAMP"] {
            let g = sqlite_type_to_gleam(t);
            assert_eq!(g.type_name, "String", "failed for type {t}");
        }
    }

    #[test]
    fn test_affinity_rules() {
        // Contains INT → integer affinity
        let g = sqlite_type_to_gleam("UNSIGNED BIG INT");
        assert_eq!(g.type_name, "Int");

        // Contains CHAR → text affinity
        let g = sqlite_type_to_gleam("VARYING CHARACTER(255)");
        assert_eq!(g.type_name, "String");

        // Contains REAL → real affinity
        let g = sqlite_type_to_gleam("SOME REAL TYPE");
        assert_eq!(g.type_name, "Float");
    }

    #[test]
    fn test_case_insensitive() {
        let g = sqlite_type_to_gleam("integer");
        assert_eq!(g.type_name, "Int");

        let g = sqlite_type_to_gleam("Text");
        assert_eq!(g.type_name, "String");
    }
}
