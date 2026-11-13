use crate::driver::type_map::{GleamType, ParamExpr, ResolvedType, Shape, TypeContext};
use crate::options::TypeOverride;
use crate::plugin::plugin::{Column, Identifier};
use crate::utils::CodeWriter;

/// Abstraction over database-specific code generation.
///
/// Each driver knows how to:
/// - Map database column types to Gleam types, param constructors, and decoders
/// - Render parameters in the shape its Gleam library expects
/// - Generate the correct function calls for each query annotation
/// - Produce the imports and private helpers the generated module needs
pub trait Driver {
    /// Module alias for code references (last segment of import path).
    /// E.g., "postgleam", "glite", "pool" or "pg" (for "db/pg").
    fn module_name(&self) -> &str;

    /// Full import path for the main module. E.g., "postgleam" or "db/pg".
    fn import_path(&self) -> &str;

    /// Full import path for the decode module. E.g., "postgleam/decode".
    fn decode_import_path(&self) -> &str;

    /// Resolve a database column to its Gleam type, param expression and decoder.
    fn resolve_column_type(&self, col: &Column, ctx: &TypeContext) -> ResolvedType;

    /// Map a Gleam type name from an override to its base type.
    /// Override names are validated when options are parsed.
    fn override_base(&self, type_name: &str) -> GleamType;

    /// True if the driver maps this database type itself, rather than falling
    /// back to String with a warning.
    fn knows_type(&self, type_name: &str, ctx: &TypeContext) -> bool;

    /// Resolve a type override to a full type. `Options::parse` made sure
    /// exactly one of `gleamType` and `dbType` is set.
    fn resolve_override(&self, o: &TypeOverride, ctx: &TypeContext) -> ResolvedType {
        match (&o.gleam_type, &o.db_type) {
            (Some(g), _) => ResolvedType::from_base(
                self.override_base(&g.type_name),
                Shape::scalar(!g.not_null),
                None,
            ),
            (None, Some(d)) => self.resolve_column_type(
                &Column {
                    not_null: d.not_null,
                    is_array: d.array,
                    r#type: Some(Identifier {
                        name: d.type_name.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ctx,
            ),
            (None, None) => unreachable!(
                "override for '{}' has neither gleamType nor dbType; Options::parse rejects that",
                o.column
            ),
        }
    }

    /// Render a parameter as one element of the driver's params list.
    /// `var` is the Gleam expression holding the value (e.g. `params.id`).
    fn render_param(&self, expr: &ParamExpr, var: &str) -> String;

    /// An expression of type `Result(Param, Error)` that must be bound before the
    /// params list is built, or `None` when the param needs no preparation.
    fn prepare_param(&self, _expr: &ParamExpr, _var: &str) -> Option<String> {
        None
    }

    /// Render a `sqlc.slice()` param as a list of params, flattened into the
    /// params list.
    fn render_slice_param(&self, expr: &ParamExpr, var: &str) -> String;

    /// Imports beyond the Gleam standard library, decided from the generated body.
    fn extra_imports(&self, body: &str) -> Vec<String>;

    /// Private helper functions used by the generated body.
    fn write_helpers(&self, body: &str, ctx: &TypeContext, w: &mut CodeWriter);

    fn write_one_call(&self, sql: &str, params: &str, w: &mut CodeWriter);
    fn write_many_call(&self, sql: &str, params: &str, w: &mut CodeWriter);
    fn write_exec_call(&self, sql: &str, params: &str, w: &mut CodeWriter);
    fn write_execrows_call(&self, sql: &str, params: &str, w: &mut CodeWriter);
    /// `param_sets` is a Gleam expression of type `List(List(Param))`.
    fn write_copyfrom_call(&self, sql: &str, param_sets: &str, w: &mut CodeWriter);
}

/// Extract the last path segment from a module path for use as the Gleam alias.
/// E.g., "postgleam" → "postgleam", "db/pg" → "pg", "my/lib/decode" → "decode".
pub fn path_alias(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

/// True if `body` references `module.` as a qualified name (not as part of a
/// longer identifier such as `my_list.`). Lines with string constants and doc
/// comments are ignored so SQL text cannot trigger imports.
pub fn uses_module(body: &str, module: &str) -> bool {
    let needle = format!("{module}.");
    code_lines(body).any(|line| contains_token(line, &needle))
}

/// True if `body` uses `name` as a bare identifier (e.g. `Some`, `None`).
pub fn uses_name(body: &str, name: &str) -> bool {
    code_lines(body).any(|line| {
        let mut start = 0;
        while let Some(pos) = line[start..].find(name) {
            let abs = start + pos;
            let before_ok = abs == 0 || {
                let b = line.as_bytes()[abs - 1] as char;
                !(b.is_ascii_alphanumeric() || b == '_' || b == '.')
            };
            let after = line.as_bytes().get(abs + name.len()).map(|b| *b as char);
            let after_ok = !matches!(after, Some(c) if c.is_ascii_alphanumeric() || c == '_');
            if before_ok && after_ok {
                return true;
            }
            start = abs + name.len();
        }
        false
    })
}

fn code_lines(body: &str) -> impl Iterator<Item = &str> {
    body.lines().filter(|l| {
        let t = l.trim_start();
        !t.starts_with("const ") && !t.starts_with("//")
    })
}

fn contains_token(line: &str, needle: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = line[start..].find(needle) {
        let abs = start + pos;
        let before_ok = abs == 0 || {
            let b = line.as_bytes()[abs - 1] as char;
            !(b.is_ascii_alphanumeric() || b == '_' || b == '.')
        };
        if before_ok {
            return true;
        }
        start = abs + needle.len();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_usage_detection() {
        let body = "const q = \"SELECT list.x\"\n  list.map(x, f)\n  my_result.x\n";
        assert!(uses_module(body, "list"));
        assert!(!uses_module(body, "result"));
        assert!(!uses_module("const q = \"list.x\"", "list"));
        assert!(uses_name("  Some(x)", "Some"));
        assert!(!uses_name("  option.Some(x)", "Some"));
        assert!(!uses_name("  SomeThing(x)", "Some"));
        assert!(uses_name("[None]", "None"));
    }
}
