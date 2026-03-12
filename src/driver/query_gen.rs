use std::collections::HashMap;

use crate::codegen::TableMap;
use crate::driver::traits::Driver;
use crate::driver::type_map::ResolvedType;
use crate::generators::naming::{column_to_field_name, escape_reserved, query_to_fn_name, to_pascal_case};
use crate::options::TypeOverride;
use crate::plugin::plugin::{Column, Query};
use crate::utils::CodeWriter;

struct ParamInfo {
    name: String,
    resolved: ResolvedType,
    is_slice: bool,
    /// The marker name from /*SLICE:xxx*/? in the SQL text.
    slice_marker: String,
}

struct ColumnInfo {
    field_name: String,
    resolved: ResolvedType,
}

/// Generate a Gleam function for a single query.
pub fn generate_query_fn(query: &Query, table_map: &TableMap, overrides: &[TypeOverride], w: &mut CodeWriter, driver: &dyn Driver) {
    let fn_name = escape_reserved(&query_to_fn_name(&query.name));
    let cmd = query.cmd.as_str();

    let params = resolve_params(&query.params, &query.text, driver);
    let columns = resolve_columns(&query.columns, table_map, &query.name, overrides, driver);
    let has_slices = params.iter().any(|p| p.is_slice);

    let const_name = format!("{fn_name}_sql");
    w.writef(format_args!(
        "const {const_name} = \"{}\"",
        escape_gleam_string(&query.text)
    ));
    w.blank();

    let row_type_name = format!("{}Row", to_pascal_case(&query.name));

    let needs_row_type =
        !columns.is_empty() && (cmd == ":one" || cmd == ":many" || cmd == ":execlastid");
    if needs_row_type {
        generate_row_type(&row_type_name, &columns, w);
        w.blank();
    }

    let params_type_name = format!("{}Params", to_pascal_case(&query.name));

    if cmd == ":copyfrom" {
        // :copyfrom always generates a params type (even for 1 param)
        if !params.is_empty() {
            generate_params_type(&params_type_name, &params, w);
            w.blank();
        }
        let sig = format!("{fn_name}(conn, rows: List({params_type_name}))");
        w.writef(format_args!("pub fn {sig} {{"));
        w.indent();
        generate_copyfrom_body(&const_name, &params, w, driver);
        w.dedent();
        w.line("}");
        return;
    }

    if params.len() >= 2 {
        generate_params_type(&params_type_name, &params, w);
        w.blank();
    }

    let sig = build_fn_signature(&fn_name, &params, &params_type_name);
    w.writef(format_args!("pub fn {sig} {{"));
    w.indent();

    // For slice queries, emit SQL expansion and flattened params
    let sql_var = if has_slices {
        emit_slice_expansion(&const_name, &params, driver.module_name(), w);
        "sql"
    } else {
        &const_name
    };

    match cmd {
        ":one" => generate_one_body(sql_var, has_slices, &params, &columns, &row_type_name, w, driver),
        ":many" => generate_many_body(sql_var, has_slices, &params, &columns, &row_type_name, w, driver),
        ":exec" => generate_exec_body(sql_var, has_slices, &params, w, driver),
        ":execrows" => generate_execrows_body(sql_var, has_slices, &params, w, driver),
        ":execlastid" => {
            if columns.is_empty() {
                generate_exec_body(sql_var, has_slices, &params, w, driver);
            } else {
                generate_one_body(sql_var, has_slices, &params, &columns, &row_type_name, w, driver);
            }
        }
        _ => {
            eprintln!("warning: unknown query command '{cmd}' for query '{}'", query.name);
            generate_exec_body(sql_var, has_slices, &params, w, driver);
        }
    }

    w.dedent();
    w.line("}");
}

fn resolve_params(params: &[crate::plugin::plugin::Parameter], sql_text: &str, driver: &dyn Driver) -> Vec<ParamInfo> {
    // Extract slice marker names from the SQL text: /*SLICE:xxx*/?
    let slice_markers = extract_slice_markers(sql_text);
    let mut slice_idx = 0;

    params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let col = p.column.as_ref().unwrap();
            let is_slice = col.is_sqlc_slice;

            let raw_name = if !col.original_name.is_empty() {
                column_to_field_name(&col.original_name)
            } else if !col.name.is_empty() {
                column_to_field_name(&col.name)
            } else {
                format!("param_{}", i + 1)
            };

            let slice_marker = if is_slice {
                let marker = slice_markers.get(slice_idx).cloned().unwrap_or_else(|| raw_name.clone());
                slice_idx += 1;
                marker
            } else {
                String::new()
            };

            let mut resolved = driver.resolve_param_type(col);

            // For slice params, wrap the type in List() and change the param expression
            if is_slice {
                resolved.type_expr = format!("List({})", resolved.type_expr);
            }

            ParamInfo {
                name: escape_reserved(&raw_name),
                resolved,
                is_slice,
                slice_marker,
            }
        })
        .collect()
}

/// Extract slice marker names from SQL text (e.g., "ids" from /*SLICE:ids*/?)
fn extract_slice_markers(sql: &str) -> Vec<String> {
    let mut markers = Vec::new();
    let pattern = "/*SLICE:";
    let mut pos = 0;
    while let Some(start) = sql[pos..].find(pattern) {
        let marker_start = pos + start + pattern.len();
        if let Some(end) = sql[marker_start..].find("*/") {
            markers.push(sql[marker_start..marker_start + end].to_string());
            pos = marker_start + end;
        } else {
            break;
        }
    }
    markers
}

fn resolve_columns(columns: &[Column], table_map: &TableMap, query_name: &str, overrides: &[TypeOverride], driver: &dyn Driver) -> Vec<ColumnInfo> {
    let mut result = Vec::new();
    let mut seen_names: HashMap<String, usize> = HashMap::new();

    for col in columns {
        let has_embed = col
            .embed_table
            .as_ref()
            .is_some_and(|t| !t.name.is_empty());

        if has_embed {
            let embed = col.embed_table.as_ref().unwrap();
            let prefix = if !col.table_alias.is_empty() {
                column_to_field_name(&col.table_alias)
            } else {
                column_to_field_name(&embed.name)
            };

            // Look up the table in the catalog to expand embed columns
            if let Some(table) = table_map.get(&embed.name) {
                for table_col in &table.columns {
                    let col_name = column_to_field_name(&table_col.name);
                    let base_name = format!("{prefix}_{col_name}");
                    let field_name =
                        escape_reserved(&deduplicate_name(&base_name, &mut seen_names));
                    result.push(ColumnInfo {
                        field_name,
                        resolved: driver.resolve_column_type(table_col),
                    });
                }
            } else {
                // Fallback: embed table not found in catalog, treat as opaque
                let base_name = deduplicate_name(&prefix, &mut seen_names);
                result.push(ColumnInfo {
                    field_name: escape_reserved(&base_name),
                    resolved: driver.resolve_column_type(col),
                });
            }
        } else {
            let base_name = escape_reserved(&column_to_field_name(&col.name));
            let field_name = deduplicate_name(&base_name, &mut seen_names);
            let resolved = if let Some(overridden) = find_override(overrides, query_name, &col.name, driver) {
                overridden
            } else {
                // Restore nullability from the source table column when sqlc loses it
                // through type casts (e.g., `nullable_col::text` loses nullability info).
                let effective_col = restore_nullability(col, table_map);
                driver.resolve_column_type(&effective_col)
            };
            result.push(ColumnInfo {
                field_name,
                resolved,
            });
        }
    }

    result
}

/// If a column has a source table reference, look up the original column definition
/// in the catalog to restore its nullability. This fixes the common case where
/// `nullable_col::text` causes sqlc to mark the result as NOT NULL.
fn restore_nullability(col: &Column, table_map: &TableMap) -> Column {
    // If already nullable, nothing to restore
    if !col.not_null {
        return col.clone();
    }

    // Need a table reference to look up the original column
    let table_ref = match &col.table {
        Some(t) if !t.name.is_empty() => t,
        _ => return col.clone(),
    };

    // Look up the table, trying both plain name and schema-qualified
    let table = table_map.get(&table_ref.name).or_else(|| {
        if !table_ref.schema.is_empty() {
            table_map.get(&format!("{}.{}", table_ref.schema, table_ref.name))
        } else {
            None
        }
    });

    if let Some(table) = table {
        // Find the original column by name
        let orig_name = if !col.original_name.is_empty() {
            &col.original_name
        } else {
            &col.name
        };
        if let Some(orig_col) = table.columns.iter().find(|c| c.name == *orig_name) {
            if !orig_col.not_null {
                // Original column IS nullable — restore that on the result
                let mut fixed = col.clone();
                fixed.not_null = false;
                return fixed;
            }
        }
    }

    col.clone()
}

/// Find and apply a type override for a specific query+column combination.
fn find_override(overrides: &[TypeOverride], query_name: &str, column_name: &str, driver: &dyn Driver) -> Option<ResolvedType> {
    overrides.iter().find_map(|o| {
        let parts: Vec<&str> = o.column.splitn(2, ':').collect();
        if parts.len() != 2 {
            return None;
        }
        let (qname, cname) = (parts[0], parts[1]);
        if (qname == "*" || qname == query_name) && cname == column_name {
            if let Some(ref gleam_type) = o.gleam_type {
                driver.resolve_override(&gleam_type.type_name, gleam_type.not_null)
            } else {
                None
            }
        } else {
            None
        }
    })
}

fn deduplicate_name(name: &str, seen: &mut HashMap<String, usize>) -> String {
    let count = seen.entry(name.to_string()).or_insert(0);
    *count += 1;
    if *count == 1 {
        name.to_string()
    } else {
        format!("{name}_{count}")
    }
}

fn generate_row_type(name: &str, columns: &[ColumnInfo], w: &mut CodeWriter) {
    w.writef(format_args!("pub type {name} {{"));
    w.indent();
    let fields: Vec<String> = columns
        .iter()
        .map(|c| format!("{}: {}", c.field_name, c.resolved.type_expr))
        .collect();
    w.writef(format_args!("{name}({})", fields.join(", ")));
    w.dedent();
    w.line("}");
}

fn generate_params_type(name: &str, params: &[ParamInfo], w: &mut CodeWriter) {
    w.writef(format_args!("pub type {name} {{"));
    w.indent();
    let fields: Vec<String> = params
        .iter()
        .map(|p| format!("{}: {}", p.name, p.resolved.type_expr))
        .collect();
    w.writef(format_args!("{name}({})", fields.join(", ")));
    w.dedent();
    w.line("}");
}

fn build_fn_signature(fn_name: &str, params: &[ParamInfo], params_type_name: &str) -> String {
    match params.len() {
        0 => format!("{fn_name}(conn)"),
        1 => {
            let p = &params[0];
            format!("{fn_name}(conn, {}: {})", p.name, p.resolved.type_expr)
        }
        _ => format!("{fn_name}(conn, params: {params_type_name})"),
    }
}

fn generate_decoder(columns: &[ColumnInfo], row_type_name: &str, w: &mut CodeWriter) {
    w.line("let decoder = {");
    w.indent();
    for (i, col) in columns.iter().enumerate() {
        w.writef(format_args!(
            "use {} <- decode.element({i}, {})",
            col.field_name,
            col.resolved.decoder_expr.to_gleam()
        ));
    }
    let field_names: Vec<&str> = columns.iter().map(|c| c.field_name.as_str()).collect();
    w.writef(format_args!(
        "decode.success({row_type_name}({}))",
        field_names
            .iter()
            .map(|f| format!("{f}:"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    w.dedent();
    w.line("}");
}

fn params_list_inline(params: &[ParamInfo], module_name: &str) -> String {
    if params.is_empty() {
        return "[]".into();
    }
    let use_params_prefix = params.len() >= 2;
    let exprs: Vec<String> = params
        .iter()
        .map(|p| {
            let var = if use_params_prefix {
                format!("params.{}", p.name)
            } else {
                p.name.clone()
            };
            p.resolved.param_expr.to_gleam(&var, module_name)
        })
        .collect();
    format!("[{}]", exprs.join(", "))
}

/// Emit SQL expansion code for slice parameters.
/// Generates `let sql = ...` and `let params = list.flatten([...])`.
fn emit_slice_expansion(
    const_name: &str,
    params: &[ParamInfo],
    module_name: &str,
    w: &mut CodeWriter,
) {
    // Build SQL expansion: replace /*SLICE:name*/? with ?,?,?
    let slice_params: Vec<&ParamInfo> = params.iter().filter(|p| p.is_slice).collect();
    if slice_params.len() == 1 {
        let sp = slice_params[0];
        let var = if params.len() >= 2 {
            format!("params.{}", sp.name)
        } else {
            sp.name.clone()
        };
        w.writef(format_args!(
            "let sql = string.replace({const_name}, \"/*SLICE:{}*/?\", string.join(list.repeat(\"?\", list.length({var})), \", \"))",
            sp.slice_marker
        ));
    } else {
        w.writef(format_args!("let sql = {const_name}"));
        for sp in &slice_params {
            let var = if params.len() >= 2 {
                format!("params.{}", sp.name)
            } else {
                sp.name.clone()
            };
            w.writef(format_args!(
                "let sql = string.replace(sql, \"/*SLICE:{}*/?\", string.join(list.repeat(\"?\", list.length({var})), \", \"))",
                sp.slice_marker
            ));
        }
    }

    // Build flattened params list
    let use_prefix = params.len() >= 2;
    let mut parts: Vec<String> = Vec::new();
    for p in params {
        let var = if use_prefix {
            format!("params.{}", p.name)
        } else {
            p.name.clone()
        };
        if p.is_slice {
            parts.push(format!("list.map({var}, {module_name}.{inner})",
                inner = extract_simple_fn_name(&p.resolved.param_expr)));
        } else {
            parts.push(format!("[{}]", p.resolved.param_expr.to_gleam(&var, module_name)));
        }
    }
    w.writef(format_args!("let params = list.flatten([{}])", parts.join(", ")));
}

/// Extract the simple function name from a ParamExpr::Direct for use in list.map.
fn extract_simple_fn_name(param_expr: &crate::driver::type_map::ParamExpr) -> String {
    match param_expr {
        crate::driver::type_map::ParamExpr::Direct { fn_name } => {
            // fn_name is like "glite.int" — extract just "int"
            fn_name.rsplit('.').next().unwrap_or(fn_name).to_string()
        }
        _ => "text".to_string(), // fallback
    }
}

fn generate_one_body(
    sql_var: &str,
    has_slices: bool,
    params: &[ParamInfo],
    columns: &[ColumnInfo],
    row_type_name: &str,
    w: &mut CodeWriter,
    driver: &dyn Driver,
) {
    generate_decoder(columns, row_type_name, w);
    let params_str = if has_slices { "params".to_string() } else { params_list_inline(params, driver.module_name()) };
    driver.write_one_call(sql_var, &params_str, w);
}

fn generate_many_body(
    sql_var: &str,
    has_slices: bool,
    params: &[ParamInfo],
    columns: &[ColumnInfo],
    row_type_name: &str,
    w: &mut CodeWriter,
    driver: &dyn Driver,
) {
    generate_decoder(columns, row_type_name, w);
    let params_str = if has_slices { "params".to_string() } else { params_list_inline(params, driver.module_name()) };
    driver.write_many_call(sql_var, &params_str, w);
}

fn generate_exec_body(
    sql_var: &str,
    has_slices: bool,
    params: &[ParamInfo],
    w: &mut CodeWriter,
    driver: &dyn Driver,
) {
    let params_str = if has_slices { "params".to_string() } else { params_list_inline(params, driver.module_name()) };
    driver.write_exec_call(sql_var, &params_str, w);
}

fn copyfrom_params_list(params: &[ParamInfo], module_name: &str) -> String {
    if params.is_empty() {
        return "[]".into();
    }
    let exprs: Vec<String> = params
        .iter()
        .map(|p| {
            let var = format!("row.{}", p.name);
            p.resolved.param_expr.to_gleam(&var, module_name)
        })
        .collect();
    format!("[{}]", exprs.join(", "))
}

fn generate_copyfrom_body(
    const_name: &str,
    params: &[ParamInfo],
    w: &mut CodeWriter,
    driver: &dyn Driver,
) {
    let params_str = copyfrom_params_list(params, driver.module_name());
    w.line("list.try_each(rows, fn(row) {");
    w.indent();
    driver.write_exec_call(const_name, &params_str, w);
    w.line("|> result.map(fn(_) { Nil })");
    w.dedent();
    w.line("})");
}

fn generate_execrows_body(
    sql_var: &str,
    has_slices: bool,
    params: &[ParamInfo],
    w: &mut CodeWriter,
    driver: &dyn Driver,
) {
    let params_str = if has_slices { "params".to_string() } else { params_list_inline(params, driver.module_name()) };
    driver.write_execrows_call(sql_var, &params_str, w);
}

fn escape_gleam_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
