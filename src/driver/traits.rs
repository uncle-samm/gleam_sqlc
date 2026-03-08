use crate::driver::type_map::{DecoderExpr, GleamType, ParamExpr, ResolvedType};
use crate::plugin::plugin::Column;
use crate::utils::CodeWriter;

/// Abstraction over database-specific code generation.
///
/// Each driver knows how to:
/// - Map database column types to Gleam types, param constructors, and decoders
/// - Generate the correct function calls for each query annotation
/// - Produce the right imports for the target Gleam database library
pub trait Driver {
    /// Module name for imports and function calls (e.g., "postgleam", "glite").
    fn module_name(&self) -> &str;

    /// Resolve a database column to its Gleam type, param expression, and decoder expression.
    fn resolve_column_type(&self, col: &Column) -> ResolvedType;

    /// Resolve a column type specifically for use as a query parameter.
    /// Defaults to `resolve_column_type`. Override to handle cases where
    /// param encoding differs from result decoding (e.g., array params
    /// use postgleam.array() but array result columns fall back to text).
    fn resolve_param_type(&self, col: &Column) -> ResolvedType {
        self.resolve_column_type(col)
    }

    /// Generate imports for the query module.
    fn write_imports(&self, needs_option: bool, needs_execrows: bool, needs_array: bool, needs_copyfrom: bool, needs_slice: bool, w: &mut CodeWriter) {
        let needs_tag_parse = needs_execrows && self.execrows_needs_tag_parse();
        if needs_tag_parse {
            w.line("import gleam/int");
        }
        if needs_tag_parse || needs_array || needs_copyfrom || needs_slice {
            w.line("import gleam/list");
        }
        if needs_option || needs_array {
            w.line("import gleam/option.{type Option}");
        }
        if needs_tag_parse || needs_copyfrom {
            w.line("import gleam/result");
        }
        if needs_tag_parse || needs_slice {
            w.line("import gleam/string");
        }
        let module = self.module_name();
        w.writef(format_args!("import {module}"));
        w.writef(format_args!("import {module}/decode"));
    }

    /// Map a Gleam type name from an override to a ResolvedType.
    /// Returns None if the type name is not recognized.
    fn resolve_override(&self, type_name: &str, not_null: bool) -> Option<ResolvedType> {
        let module = self.module_name();
        let base = match type_name {
            "String" => GleamType::simple("String", &format!("{module}.text"), "decode.text"),
            "Int" => GleamType::simple("Int", &format!("{module}.int"), "decode.int"),
            "Float" => GleamType::simple("Float", &format!("{module}.float"), "decode.float"),
            "Bool" => GleamType::simple("Bool", &format!("{module}.bool"), "decode.bool"),
            "BitArray" => GleamType::simple("BitArray", &format!("{module}.bytea"), "decode.bytea"),
            _ => return None,
        };
        if not_null {
            Some(ResolvedType {
                type_expr: base.type_name,
                param_expr: ParamExpr::Direct { fn_name: base.param_fn },
                decoder_expr: DecoderExpr::Direct { fn_name: base.decoder_fn },
            })
        } else {
            Some(ResolvedType {
                type_expr: format!("Option({})", base.type_name),
                param_expr: ParamExpr::Nullable { inner_fn: base.param_fn },
                decoder_expr: DecoderExpr::Optional { inner_fn: base.decoder_fn },
            })
        }
    }

    /// Whether :execrows needs a tag-parsing helper function.
    /// PostgreSQL returns a command tag like "UPDATE 5" that must be parsed.
    /// SQLite's exec() returns the affected row count directly.
    fn execrows_needs_tag_parse(&self) -> bool;

    /// Write the :execrows helper function if needed.
    fn write_execrows_helper(&self, w: &mut CodeWriter) {
        if !self.execrows_needs_tag_parse() {
            return;
        }
        w.line("/// Parse affected row count from a PostgreSQL command tag.");
        w.line("/// Tags look like \"UPDATE 5\", \"DELETE 2\", \"INSERT 0 1\".");
        w.line("fn parse_affected_rows(tag: String) -> Int {");
        w.indent();
        w.line("tag");
        w.indent();
        w.line("|> string.split(\" \")");
        w.line("|> list.last()");
        w.line("|> result.try(int.parse)");
        w.line("|> result.unwrap(0)");
        w.dedent();
        w.dedent();
        w.line("}");
    }

    /// Write the function call for a :one query.
    fn write_one_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        w.writef(format_args!(
            "{m}.query_one(conn, {const_name}, {params_str}, decoder)"
        ));
    }

    /// Write the function call for a :many query.
    fn write_many_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter) {
        let m = self.module_name();
        w.writef(format_args!(
            "{m}.query_with(conn, {const_name}, {params_str}, decoder)"
        ));
    }

    /// Write the function call for a :exec query.
    fn write_exec_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter);

    /// Write the function call for a :execrows query.
    fn write_execrows_call(&self, const_name: &str, params_str: &str, w: &mut CodeWriter);
}
