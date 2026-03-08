use crate::driver::traits::Driver;
use crate::driver::type_map::{DecoderExpr, GleamType, ParamExpr, ResolvedType};
use crate::plugin::plugin::Column;
use crate::utils::CodeWriter;

pub struct PostgresDriver;

impl Driver for PostgresDriver {
    fn module_name(&self) -> &str {
        "postgleam"
    }

    fn resolve_column_type(&self, col: &Column) -> ResolvedType {
        let col_type = col
            .r#type
            .as_ref()
            .map(|t| t.name.as_str())
            .unwrap_or("text");
        let base = pg_type_to_gleam(col_type);
        let is_nullable = !col.not_null;

        if col.is_array {
            // Array result columns: no decode.array in PostGleam yet, fall back to text.
            // Array params are handled in resolve_param_type().
            ResolvedType {
                type_expr: if is_nullable {
                    "Option(String)".into()
                } else {
                    "String".into()
                },
                param_expr: if is_nullable {
                    ParamExpr::Nullable {
                        inner_fn: "postgleam.text".into(),
                    }
                } else {
                    ParamExpr::Direct {
                        fn_name: "postgleam.text".into(),
                    }
                },
                decoder_expr: if is_nullable {
                    DecoderExpr::Optional {
                        inner_fn: "decode.text".into(),
                    }
                } else {
                    DecoderExpr::Direct {
                        fn_name: "decode.text".into(),
                    }
                },
            }
        } else if is_nullable {
            let param_expr = match base.param_arity {
                2 | 3 | 4 => ParamExpr::NullableDestruct {
                    fn_name: base.param_fn,
                    arity: base.param_arity,
                },
                _ => ParamExpr::Nullable {
                    inner_fn: base.param_fn,
                },
            };
            ResolvedType {
                type_expr: format!("Option({})", base.type_name),
                param_expr,
                decoder_expr: DecoderExpr::Optional {
                    inner_fn: base.decoder_fn,
                },
            }
        } else {
            let param_expr = match base.param_arity {
                2 => ParamExpr::Destruct2 {
                    fn_name: base.param_fn.clone(),
                },
                3 => ParamExpr::Destruct3 {
                    fn_name: base.param_fn.clone(),
                },
                4 => ParamExpr::Destruct4 {
                    fn_name: base.param_fn.clone(),
                },
                _ => ParamExpr::Direct {
                    fn_name: base.param_fn,
                },
            };
            ResolvedType {
                type_expr: base.type_name,
                param_expr,
                decoder_expr: DecoderExpr::Direct {
                    fn_name: base.decoder_fn,
                },
            }
        }
    }

    fn resolve_param_type(&self, col: &Column) -> ResolvedType {
        if col.is_array {
            let col_type = col
                .r#type
                .as_ref()
                .map(|t| t.name.as_str())
                .unwrap_or("text");
            let base = pg_type_to_gleam(col_type);
            let is_nullable = !col.not_null;

            // Only support arity-1 element types for array params.
            if base.param_arity > 1 {
                return self.resolve_column_type(col);
            }

            ResolvedType {
                type_expr: if is_nullable {
                    format!("Option(List({}))", base.type_name)
                } else {
                    format!("List({})", base.type_name)
                },
                param_expr: ParamExpr::Array {
                    inner_fn: base.param_fn,
                    nullable: is_nullable,
                },
                // decoder_expr unused for params
                decoder_expr: DecoderExpr::Direct {
                    fn_name: "decode.text".into(),
                },
            }
        } else {
            self.resolve_column_type(col)
        }
    }

    fn execrows_needs_tag_parse(&self) -> bool {
        true
    }

    fn write_exec_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter) {
        w.writef(format_args!(
            "postgleam.query(conn, {const_name}, {params_str})"
        ));
    }

    fn write_execrows_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter) {
        w.writef(format_args!(
            "postgleam.query(conn, {const_name}, {params_str})"
        ));
        w.line("|> result.map(fn(r) { parse_affected_rows(r.tag) })");
    }
}

/// Map a PostgreSQL type name to Gleam type info.
fn pg_type_to_gleam(pg_type: &str) -> GleamType {
    // Strip pg_catalog. prefix if present
    let type_name = pg_type
        .strip_prefix("pg_catalog.")
        .unwrap_or(pg_type)
        .to_lowercase();

    match type_name.as_str() {
        // Boolean
        "bool" | "boolean" => GleamType::simple("Bool", "postgleam.bool", "decode.bool"),

        // Integers
        "int2" | "smallint" | "smallserial" => {
            GleamType::simple("Int", "postgleam.int", "decode.int")
        }
        "int4" | "integer" | "int" | "serial" => {
            GleamType::simple("Int", "postgleam.int", "decode.int")
        }
        "int8" | "bigint" | "bigserial" => {
            GleamType::simple("Int", "postgleam.int", "decode.int")
        }

        // Floating point
        "float4" | "real" => GleamType::simple("Float", "postgleam.float", "decode.float"),
        "float8" | "double precision" | "double" => {
            GleamType::simple("Float", "postgleam.float", "decode.float")
        }

        // Numeric/Decimal
        "numeric" | "decimal" => {
            GleamType::simple("String", "postgleam.numeric", "decode.numeric")
        }

        // Money (Int in PostGleam — cents as int64)
        "money" => GleamType::simple("Int", "postgleam.money", "decode.money"),

        // Text/String
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "name" => {
            GleamType::simple("String", "postgleam.text", "decode.text")
        }

        // Binary
        "bytea" => GleamType::simple("BitArray", "postgleam.bytea", "decode.bytea"),

        // UUID
        "uuid" => GleamType::simple("BitArray", "postgleam.uuid", "decode.uuid"),

        // JSON
        "json" => GleamType::simple("String", "postgleam.json", "decode.json"),
        "jsonb" => GleamType::simple("String", "postgleam.jsonb", "decode.jsonb"),

        // Date/Time — simple (arity 1)
        "date" => GleamType::simple("Int", "postgleam.date", "decode.date"),
        "timestamp" | "timestamp without time zone" => {
            GleamType::simple("Int", "postgleam.timestamp", "decode.timestamp")
        }
        "timestamptz" | "timestamp with time zone" => {
            GleamType::simple("Int", "postgleam.timestamptz", "decode.timestamptz")
        }
        "time" | "time without time zone" => {
            GleamType::simple("Int", "postgleam.time", "decode.time")
        }

        // Date/Time — multi-arg
        "timetz" | "time with time zone" => {
            GleamType::multi("#(Int, Int)", "postgleam.timetz", "decode.timetz", 2)
        }
        "interval" => {
            GleamType::multi("#(Int, Int, Int)", "postgleam.interval", "decode.interval", 3)
        }

        // XML
        "xml" => GleamType::simple("String", "postgleam.xml", "decode.xml"),

        // JSONPATH
        "jsonpath" => GleamType::simple("String", "postgleam.jsonpath", "decode.jsonpath"),

        // Geometric types — with param constructors
        "point" => GleamType::multi("#(Float, Float)", "postgleam.point", "decode.point", 2),
        "circle" => {
            GleamType::multi("#(Float, Float, Float)", "postgleam.circle", "decode.circle", 3)
        }

        // Geometric types
        "line" => {
            GleamType::multi("#(Float, Float, Float)", "postgleam.line", "decode.line", 3)
        }
        "lseg" => {
            GleamType::multi("#(Float, Float, Float, Float)", "postgleam.lseg", "decode.lseg", 4)
        }
        "box" => {
            GleamType::multi("#(Float, Float, Float, Float)", "postgleam.box", "decode.box", 4)
        }
        "path" => {
            GleamType::multi("#(Bool, List(#(Float, Float)))", "postgleam.path", "decode.path", 2)
        }
        "polygon" => {
            GleamType::simple("List(#(Float, Float))", "postgleam.polygon", "decode.polygon")
        }

        // Network types
        "macaddr" => GleamType::simple("BitArray", "postgleam.macaddr", "decode.macaddr"),
        "macaddr8" => GleamType::simple("BitArray", "postgleam.macaddr8", "decode.macaddr8"),
        "cidr" | "inet" => {
            GleamType::multi("#(Int, BitArray, Int)", "postgleam.inet", "decode.inet", 3)
        }

        // Bit string
        "bit" | "varbit" | "bit varying" => {
            GleamType::multi("#(Int, BitArray)", "postgleam.bit_string", "decode.bit_string", 2)
        }

        // Full-text search (no binary codec support in PostGleam yet)
        "tsvector" | "tsquery" => {
            GleamType::simple("String", "postgleam.text", "decode.text")
        }

        // Void (for functions returning void)
        "void" => GleamType::simple("Nil", "postgleam.null", "decode.text"),

        // Unknown — fall back to String
        _ => {
            eprintln!("warning: unknown PostgreSQL type '{pg_type}', falling back to String");
            GleamType::simple("String", "postgleam.text", "decode.text")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_types() {
        let t = pg_type_to_gleam("bool");
        assert_eq!(t.type_name, "Bool");
        assert_eq!(t.param_fn, "postgleam.bool");
        assert_eq!(t.decoder_fn, "decode.bool");
        assert_eq!(t.param_arity, 1);

        let t = pg_type_to_gleam("int4");
        assert_eq!(t.type_name, "Int");

        let t = pg_type_to_gleam("text");
        assert_eq!(t.type_name, "String");

        let t = pg_type_to_gleam("float8");
        assert_eq!(t.type_name, "Float");

        let t = pg_type_to_gleam("uuid");
        assert_eq!(t.type_name, "BitArray");

        let t = pg_type_to_gleam("jsonb");
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.jsonb");

        let t = pg_type_to_gleam("timestamptz");
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "postgleam.timestamptz");
    }

    #[test]
    fn test_pg_catalog_prefix() {
        let t = pg_type_to_gleam("pg_catalog.int4");
        assert_eq!(t.type_name, "Int");

        let t = pg_type_to_gleam("pg_catalog.bool");
        assert_eq!(t.type_name, "Bool");
    }

    #[test]
    fn test_unknown_type_fallback() {
        let t = pg_type_to_gleam("some_custom_type");
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.text");
    }

    #[test]
    fn test_time_types() {
        let t = pg_type_to_gleam("time");
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "postgleam.time");
        assert_eq!(t.decoder_fn, "decode.time");
        assert_eq!(t.param_arity, 1);

        let t = pg_type_to_gleam("timetz");
        assert_eq!(t.type_name, "#(Int, Int)");
        assert_eq!(t.param_fn, "postgleam.timetz");
        assert_eq!(t.decoder_fn, "decode.timetz");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("interval");
        assert_eq!(t.type_name, "#(Int, Int, Int)");
        assert_eq!(t.param_fn, "postgleam.interval");
        assert_eq!(t.decoder_fn, "decode.interval");
        assert_eq!(t.param_arity, 3);
    }

    #[test]
    fn test_money_type() {
        let t = pg_type_to_gleam("money");
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "postgleam.money");
        assert_eq!(t.decoder_fn, "decode.money");
    }

    #[test]
    fn test_xml_jsonpath() {
        let t = pg_type_to_gleam("xml");
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.xml");
        assert_eq!(t.decoder_fn, "decode.xml");

        let t = pg_type_to_gleam("jsonpath");
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.jsonpath");
        assert_eq!(t.decoder_fn, "decode.jsonpath");
    }

    #[test]
    fn test_geometric_types() {
        let t = pg_type_to_gleam("point");
        assert_eq!(t.type_name, "#(Float, Float)");
        assert_eq!(t.param_fn, "postgleam.point");
        assert_eq!(t.decoder_fn, "decode.point");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("circle");
        assert_eq!(t.type_name, "#(Float, Float, Float)");
        assert_eq!(t.param_fn, "postgleam.circle");
        assert_eq!(t.decoder_fn, "decode.circle");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("line");
        assert_eq!(t.type_name, "#(Float, Float, Float)");
        assert_eq!(t.param_fn, "postgleam.line");
        assert_eq!(t.decoder_fn, "decode.line");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("lseg");
        assert_eq!(t.type_name, "#(Float, Float, Float, Float)");
        assert_eq!(t.param_fn, "postgleam.lseg");
        assert_eq!(t.decoder_fn, "decode.lseg");
        assert_eq!(t.param_arity, 4);

        let t = pg_type_to_gleam("box");
        assert_eq!(t.param_fn, "postgleam.box");
        assert_eq!(t.decoder_fn, "decode.box");
        assert_eq!(t.param_arity, 4);

        let t = pg_type_to_gleam("path");
        assert_eq!(t.type_name, "#(Bool, List(#(Float, Float)))");
        assert_eq!(t.param_fn, "postgleam.path");
        assert_eq!(t.decoder_fn, "decode.path");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("polygon");
        assert_eq!(t.type_name, "List(#(Float, Float))");
        assert_eq!(t.param_fn, "postgleam.polygon");
        assert_eq!(t.decoder_fn, "decode.polygon");
    }

    #[test]
    fn test_network_types() {
        let t = pg_type_to_gleam("macaddr");
        assert_eq!(t.type_name, "BitArray");
        assert_eq!(t.param_fn, "postgleam.macaddr");
        assert_eq!(t.decoder_fn, "decode.macaddr");

        let t = pg_type_to_gleam("macaddr8");
        assert_eq!(t.type_name, "BitArray");
        assert_eq!(t.param_fn, "postgleam.macaddr8");
        assert_eq!(t.decoder_fn, "decode.macaddr8");

        let t = pg_type_to_gleam("inet");
        assert_eq!(t.type_name, "#(Int, BitArray, Int)");
        assert_eq!(t.param_fn, "postgleam.inet");
        assert_eq!(t.decoder_fn, "decode.inet");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("cidr");
        assert_eq!(t.type_name, "#(Int, BitArray, Int)");
        assert_eq!(t.param_fn, "postgleam.inet");
        assert_eq!(t.decoder_fn, "decode.inet");
    }

    #[test]
    fn test_bit_string_type() {
        let t = pg_type_to_gleam("bit");
        assert_eq!(t.type_name, "#(Int, BitArray)");
        assert_eq!(t.param_fn, "postgleam.bit_string");
        assert_eq!(t.decoder_fn, "decode.bit_string");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("varbit");
        assert_eq!(t.type_name, "#(Int, BitArray)");
        assert_eq!(t.param_fn, "postgleam.bit_string");
        assert_eq!(t.decoder_fn, "decode.bit_string");
    }

    #[test]
    fn test_multi_arity_resolve() {
        let driver = PostgresDriver;

        // Non-nullable point (arity 2) should use Destruct2
        let col = Column {
            not_null: true,
            is_array: false,
            r#type: Some(crate::plugin::plugin::Identifier {
                name: "point".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = driver.resolve_column_type(&col);
        assert_eq!(resolved.type_expr, "#(Float, Float)");
        assert!(matches!(resolved.param_expr, ParamExpr::Destruct2 { .. }));

        // Nullable interval (arity 3) should use NullableDestruct
        let col = Column {
            not_null: false,
            is_array: false,
            r#type: Some(crate::plugin::plugin::Identifier {
                name: "interval".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = driver.resolve_column_type(&col);
        assert_eq!(resolved.type_expr, "Option(#(Int, Int, Int))");
        assert!(matches!(
            resolved.param_expr,
            ParamExpr::NullableDestruct { arity: 3, .. }
        ));

        // Simple type (arity 1) should use Direct/Nullable as before
        let col = Column {
            not_null: true,
            is_array: false,
            r#type: Some(crate::plugin::plugin::Identifier {
                name: "int4".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = driver.resolve_column_type(&col);
        assert_eq!(resolved.type_expr, "Int");
        assert!(matches!(resolved.param_expr, ParamExpr::Direct { .. }));
    }
}
