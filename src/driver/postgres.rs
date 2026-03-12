use crate::driver::traits::{path_alias, Driver};
use crate::driver::type_map::{DecoderExpr, GleamType, ParamExpr, ResolvedType};
use crate::options::Options;
use crate::plugin::plugin::Column;
use crate::utils::CodeWriter;

pub struct PostgresDriver {
    /// Full import path, e.g., "postgleam" or "db/pg"
    import: String,
    /// Module alias (last segment), e.g., "postgleam" or "pg"
    alias: String,
    /// Full decode import path, e.g., "postgleam/decode" or "db/pg/decode"
    decode_import: String,
    /// Map uuid → String (text param/decoder) instead of BitArray
    uuid_as_string: bool,
}

impl PostgresDriver {
    pub fn new(options: &Options) -> Self {
        let import = options
            .module
            .clone()
            .unwrap_or_else(|| "postgleam".to_string());
        let alias = path_alias(&import);
        let decode_import = options
            .decode_module
            .clone()
            .unwrap_or_else(|| format!("{import}/decode"));
        Self {
            import,
            alias,
            decode_import,
            uuid_as_string: options.uuid_as_string,
        }
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

    fn resolve_column_type(&self, col: &Column) -> ResolvedType {
        let col_type = col
            .r#type
            .as_ref()
            .map(|t| t.name.as_str())
            .unwrap_or("text");
        let base = pg_type_to_gleam(col_type, &self.alias, self.uuid_as_string);
        let is_nullable = !col.not_null;

        if col.is_array {
            // Array result columns: no decode.array in PostGleam yet, fall back to text.
            // Array params are handled in resolve_param_type().
            let m = &self.alias;
            ResolvedType {
                type_expr: if is_nullable {
                    "Option(String)".into()
                } else {
                    "String".into()
                },
                param_expr: if is_nullable {
                    ParamExpr::Nullable {
                        inner_fn: format!("{m}.text"),
                    }
                } else {
                    ParamExpr::Direct {
                        fn_name: format!("{m}.text"),
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
            let base = pg_type_to_gleam(col_type, &self.alias, self.uuid_as_string);
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
        let m = self.module_name();
        w.writef(format_args!(
            "{m}.query(conn, {const_name}, {params_str})"
        ));
    }

    fn write_execrows_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        w.writef(format_args!(
            "{m}.query(conn, {const_name}, {params_str})"
        ));
        w.line("|> result.map(fn(r) { parse_affected_rows(r.tag) })");
    }
}

/// Map a PostgreSQL type name to Gleam type info.
/// The `module` parameter is the module alias used in param constructors
/// (e.g., "postgleam" or "pg").
fn pg_type_to_gleam(pg_type: &str, module: &str, uuid_as_string: bool) -> GleamType {
    // Strip pg_catalog. prefix if present
    let type_name = pg_type
        .strip_prefix("pg_catalog.")
        .unwrap_or(pg_type)
        .to_lowercase();

    match type_name.as_str() {
        // Boolean
        "bool" | "boolean" => GleamType::simple("Bool", &format!("{module}.bool"), "decode.bool"),

        // Integers
        "int2" | "smallint" | "smallserial" => {
            GleamType::simple("Int", &format!("{module}.int"), "decode.int")
        }
        "int4" | "integer" | "int" | "serial" => {
            GleamType::simple("Int", &format!("{module}.int"), "decode.int")
        }
        "int8" | "bigint" | "bigserial" => {
            GleamType::simple("Int", &format!("{module}.int"), "decode.int")
        }

        // Floating point
        "float4" | "real" => GleamType::simple("Float", &format!("{module}.float"), "decode.float"),
        "float8" | "double precision" | "double" => {
            GleamType::simple("Float", &format!("{module}.float"), "decode.float")
        }

        // Numeric/Decimal
        "numeric" | "decimal" => {
            GleamType::simple("String", &format!("{module}.numeric"), "decode.numeric")
        }

        // Money (Int in PostGleam — cents as int64)
        "money" => GleamType::simple("Int", &format!("{module}.money"), "decode.money"),

        // Text/String
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "name" => {
            GleamType::simple("String", &format!("{module}.text"), "decode.text")
        }

        // Binary
        "bytea" => GleamType::simple("BitArray", &format!("{module}.bytea"), "decode.bytea"),

        // UUID
        // Always use {module}.uuid for param encoding — the wire protocol needs
        // binary UUID format. uuid_as_string only affects the Gleam type (String
        // vs BitArray) and the decoder (text vs uuid).
        "uuid" => {
            if uuid_as_string {
                GleamType::simple("String", &format!("{module}.uuid"), "decode.text")
            } else {
                GleamType::simple("BitArray", &format!("{module}.uuid"), "decode.uuid")
            }
        }

        // JSON
        "json" => GleamType::simple("String", &format!("{module}.json"), "decode.json"),
        "jsonb" => GleamType::simple("String", &format!("{module}.jsonb"), "decode.jsonb"),

        // Date/Time — simple (arity 1)
        "date" => GleamType::simple("Int", &format!("{module}.date"), "decode.date"),
        "timestamp" | "timestamp without time zone" => {
            GleamType::simple("Int", &format!("{module}.timestamp"), "decode.timestamp")
        }
        "timestamptz" | "timestamp with time zone" => {
            GleamType::simple("Int", &format!("{module}.timestamptz"), "decode.timestamptz")
        }
        "time" | "time without time zone" => {
            GleamType::simple("Int", &format!("{module}.time"), "decode.time")
        }

        // Date/Time — multi-arg
        "timetz" | "time with time zone" => {
            GleamType::multi("#(Int, Int)", &format!("{module}.timetz"), "decode.timetz", 2)
        }
        "interval" => {
            GleamType::multi("#(Int, Int, Int)", &format!("{module}.interval"), "decode.interval", 3)
        }

        // XML
        "xml" => GleamType::simple("String", &format!("{module}.xml"), "decode.xml"),

        // JSONPATH
        "jsonpath" => GleamType::simple("String", &format!("{module}.jsonpath"), "decode.jsonpath"),

        // Geometric types — with param constructors
        "point" => GleamType::multi("#(Float, Float)", &format!("{module}.point"), "decode.point", 2),
        "circle" => {
            GleamType::multi("#(Float, Float, Float)", &format!("{module}.circle"), "decode.circle", 3)
        }

        // Geometric types
        "line" => {
            GleamType::multi("#(Float, Float, Float)", &format!("{module}.line"), "decode.line", 3)
        }
        "lseg" => {
            GleamType::multi("#(Float, Float, Float, Float)", &format!("{module}.lseg"), "decode.lseg", 4)
        }
        "box" => {
            GleamType::multi("#(Float, Float, Float, Float)", &format!("{module}.box"), "decode.box", 4)
        }
        "path" => {
            GleamType::multi("#(Bool, List(#(Float, Float)))", &format!("{module}.path"), "decode.path", 2)
        }
        "polygon" => {
            GleamType::simple("List(#(Float, Float))", &format!("{module}.polygon"), "decode.polygon")
        }

        // Network types
        "macaddr" => GleamType::simple("BitArray", &format!("{module}.macaddr"), "decode.macaddr"),
        "macaddr8" => GleamType::simple("BitArray", &format!("{module}.macaddr8"), "decode.macaddr8"),
        "cidr" | "inet" => {
            GleamType::multi("#(Int, BitArray, Int)", &format!("{module}.inet"), "decode.inet", 3)
        }

        // Bit string
        "bit" | "varbit" | "bit varying" => {
            GleamType::multi("#(Int, BitArray)", &format!("{module}.bit_string"), "decode.bit_string", 2)
        }

        // Full-text search (no binary codec support in PostGleam yet)
        "tsvector" | "tsquery" => {
            GleamType::simple("String", &format!("{module}.text"), "decode.text")
        }

        // Void (for functions returning void)
        "void" => GleamType::simple("Nil", &format!("{module}.null"), "decode.text"),

        // Unknown — fall back to String
        _ => {
            eprintln!("warning: unknown PostgreSQL type '{pg_type}', falling back to String");
            GleamType::simple("String", &format!("{module}.text"), "decode.text")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_types() {
        let t = pg_type_to_gleam("bool", "postgleam", false);
        assert_eq!(t.type_name, "Bool");
        assert_eq!(t.param_fn, "postgleam.bool");
        assert_eq!(t.decoder_fn, "decode.bool");
        assert_eq!(t.param_arity, 1);

        let t = pg_type_to_gleam("int4", "postgleam", false);
        assert_eq!(t.type_name, "Int");

        let t = pg_type_to_gleam("text", "postgleam", false);
        assert_eq!(t.type_name, "String");

        let t = pg_type_to_gleam("float8", "postgleam", false);
        assert_eq!(t.type_name, "Float");

        let t = pg_type_to_gleam("uuid", "postgleam", false);
        assert_eq!(t.type_name, "BitArray");

        let t = pg_type_to_gleam("jsonb", "postgleam", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.jsonb");

        let t = pg_type_to_gleam("timestamptz", "postgleam", false);
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "postgleam.timestamptz");
    }

    #[test]
    fn test_custom_module() {
        let t = pg_type_to_gleam("bool", "pg", false);
        assert_eq!(t.param_fn, "pg.bool");
        assert_eq!(t.decoder_fn, "decode.bool");

        let t = pg_type_to_gleam("int4", "pg", false);
        assert_eq!(t.param_fn, "pg.int");

        let t = pg_type_to_gleam("text", "pg", false);
        assert_eq!(t.param_fn, "pg.text");

        let t = pg_type_to_gleam("jsonb", "pg", false);
        assert_eq!(t.param_fn, "pg.jsonb");
    }

    #[test]
    fn test_pg_catalog_prefix() {
        let t = pg_type_to_gleam("pg_catalog.int4", "postgleam", false);
        assert_eq!(t.type_name, "Int");

        let t = pg_type_to_gleam("pg_catalog.bool", "postgleam", false);
        assert_eq!(t.type_name, "Bool");
    }

    #[test]
    fn test_unknown_type_fallback() {
        let t = pg_type_to_gleam("some_custom_type", "postgleam", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.text");
    }

    #[test]
    fn test_time_types() {
        let t = pg_type_to_gleam("time", "postgleam", false);
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "postgleam.time");
        assert_eq!(t.decoder_fn, "decode.time");
        assert_eq!(t.param_arity, 1);

        let t = pg_type_to_gleam("timetz", "postgleam", false);
        assert_eq!(t.type_name, "#(Int, Int)");
        assert_eq!(t.param_fn, "postgleam.timetz");
        assert_eq!(t.decoder_fn, "decode.timetz");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("interval", "postgleam", false);
        assert_eq!(t.type_name, "#(Int, Int, Int)");
        assert_eq!(t.param_fn, "postgleam.interval");
        assert_eq!(t.decoder_fn, "decode.interval");
        assert_eq!(t.param_arity, 3);
    }

    #[test]
    fn test_money_type() {
        let t = pg_type_to_gleam("money", "postgleam", false);
        assert_eq!(t.type_name, "Int");
        assert_eq!(t.param_fn, "postgleam.money");
        assert_eq!(t.decoder_fn, "decode.money");
    }

    #[test]
    fn test_xml_jsonpath() {
        let t = pg_type_to_gleam("xml", "postgleam", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.xml");
        assert_eq!(t.decoder_fn, "decode.xml");

        let t = pg_type_to_gleam("jsonpath", "postgleam", false);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.jsonpath");
        assert_eq!(t.decoder_fn, "decode.jsonpath");
    }

    #[test]
    fn test_geometric_types() {
        let t = pg_type_to_gleam("point", "postgleam", false);
        assert_eq!(t.type_name, "#(Float, Float)");
        assert_eq!(t.param_fn, "postgleam.point");
        assert_eq!(t.decoder_fn, "decode.point");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("circle", "postgleam", false);
        assert_eq!(t.type_name, "#(Float, Float, Float)");
        assert_eq!(t.param_fn, "postgleam.circle");
        assert_eq!(t.decoder_fn, "decode.circle");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("line", "postgleam", false);
        assert_eq!(t.type_name, "#(Float, Float, Float)");
        assert_eq!(t.param_fn, "postgleam.line");
        assert_eq!(t.decoder_fn, "decode.line");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("lseg", "postgleam", false);
        assert_eq!(t.type_name, "#(Float, Float, Float, Float)");
        assert_eq!(t.param_fn, "postgleam.lseg");
        assert_eq!(t.decoder_fn, "decode.lseg");
        assert_eq!(t.param_arity, 4);

        let t = pg_type_to_gleam("box", "postgleam", false);
        assert_eq!(t.param_fn, "postgleam.box");
        assert_eq!(t.decoder_fn, "decode.box");
        assert_eq!(t.param_arity, 4);

        let t = pg_type_to_gleam("path", "postgleam", false);
        assert_eq!(t.type_name, "#(Bool, List(#(Float, Float)))");
        assert_eq!(t.param_fn, "postgleam.path");
        assert_eq!(t.decoder_fn, "decode.path");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("polygon", "postgleam", false);
        assert_eq!(t.type_name, "List(#(Float, Float))");
        assert_eq!(t.param_fn, "postgleam.polygon");
        assert_eq!(t.decoder_fn, "decode.polygon");
    }

    #[test]
    fn test_network_types() {
        let t = pg_type_to_gleam("macaddr", "postgleam", false);
        assert_eq!(t.type_name, "BitArray");
        assert_eq!(t.param_fn, "postgleam.macaddr");
        assert_eq!(t.decoder_fn, "decode.macaddr");

        let t = pg_type_to_gleam("macaddr8", "postgleam", false);
        assert_eq!(t.type_name, "BitArray");
        assert_eq!(t.param_fn, "postgleam.macaddr8");
        assert_eq!(t.decoder_fn, "decode.macaddr8");

        let t = pg_type_to_gleam("inet", "postgleam", false);
        assert_eq!(t.type_name, "#(Int, BitArray, Int)");
        assert_eq!(t.param_fn, "postgleam.inet");
        assert_eq!(t.decoder_fn, "decode.inet");
        assert_eq!(t.param_arity, 3);

        let t = pg_type_to_gleam("cidr", "postgleam", false);
        assert_eq!(t.type_name, "#(Int, BitArray, Int)");
        assert_eq!(t.param_fn, "postgleam.inet");
        assert_eq!(t.decoder_fn, "decode.inet");
    }

    #[test]
    fn test_bit_string_type() {
        let t = pg_type_to_gleam("bit", "postgleam", false);
        assert_eq!(t.type_name, "#(Int, BitArray)");
        assert_eq!(t.param_fn, "postgleam.bit_string");
        assert_eq!(t.decoder_fn, "decode.bit_string");
        assert_eq!(t.param_arity, 2);

        let t = pg_type_to_gleam("varbit", "postgleam", false);
        assert_eq!(t.type_name, "#(Int, BitArray)");
        assert_eq!(t.param_fn, "postgleam.bit_string");
        assert_eq!(t.decoder_fn, "decode.bit_string");
    }

    #[test]
    fn test_multi_arity_resolve() {
        let options = Options::default();
        let driver = PostgresDriver::new(&options);

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

        // Param expressions should use alias
        let col = Column {
            not_null: true,
            is_array: false,
            r#type: Some(crate::plugin::plugin::Identifier {
                name: "text".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = driver.resolve_column_type(&col);
        match &resolved.param_expr {
            ParamExpr::Direct { fn_name } => assert_eq!(fn_name, "pg.text"),
            other => panic!("expected Direct, got {:?}", other),
        }
    }

    #[test]
    fn test_uuid_as_string() {
        let t = pg_type_to_gleam("uuid", "postgleam", true);
        assert_eq!(t.type_name, "String");
        assert_eq!(t.param_fn, "postgleam.uuid"); // always uuid for wire protocol
        assert_eq!(t.decoder_fn, "decode.text");

        // Default (false) should still be BitArray
        let t = pg_type_to_gleam("uuid", "postgleam", false);
        assert_eq!(t.type_name, "BitArray");
        assert_eq!(t.param_fn, "postgleam.uuid");
    }

    #[test]
    fn test_custom_decode_module() {
        let options = Options {
            module: Some("db/pg".to_string()),
            decode_module: Some("db/pg/decoders".to_string()),
            ..Default::default()
        };
        let driver = PostgresDriver::new(&options);
        assert_eq!(driver.decode_import_path(), "db/pg/decoders");
    }
}
