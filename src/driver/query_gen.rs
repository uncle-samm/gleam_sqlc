use std::collections::HashMap;

use crate::codegen::TableMap;
use crate::driver::traits::Driver;
use crate::driver::type_map::ResolvedType;
use crate::generators::naming::{column_to_field_name, escape_reserved, query_to_fn_name, to_pascal_case};
use crate::options::TypeOverride;
use crate::plugin::plugin::{Column, Identifier, Query};
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

    let params = resolve_params(&query.params, &query.text, table_map, &query.insert_into_table, driver);
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

fn resolve_params(
    params: &[crate::plugin::plugin::Parameter],
    sql_text: &str,
    table_map: &TableMap,
    insert_into_table: &Option<Identifier>,
    driver: &dyn Driver,
) -> Vec<ParamInfo> {
    // Extract slice marker names from the SQL text: /*SLICE:xxx*/?
    let slice_markers = extract_slice_markers(sql_text);
    let mut slice_idx = 0;

    // For INSERT queries where sqlc strips column names from cast params (e.g. $5::uuid),
    // build a mapping from $N -> column name by parsing the INSERT column list and VALUES.
    let insert_param_names = build_insert_param_map(sql_text, insert_into_table);

    params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let col = p.column.as_ref().unwrap();
            let is_slice = col.is_sqlc_slice;

            // Try to recover the column name from the INSERT mapping if sqlc stripped it
            let recovered_name = if col.name.is_empty() && col.original_name.is_empty() {
                insert_param_names.get(&p.number).cloned()
            } else {
                None
            };

            let raw_name = if !col.original_name.is_empty() {
                column_to_field_name(&col.original_name)
            } else if !col.name.is_empty() {
                column_to_field_name(&col.name)
            } else if let Some(ref name) = recovered_name {
                column_to_field_name(name)
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

            // Restore nullability from the source table column.
            // If we recovered a column name from the INSERT mapping, inject it into the
            // column so restore_nullability can look it up in the table catalog.
            let effective_col = if let Some(ref name) = recovered_name {
                let mut patched = col.clone();
                patched.name = name.clone();
                if patched.table.is_none() {
                    if let Some(table_id) = insert_into_table {
                        patched.table = Some(table_id.clone());
                    }
                }
                restore_nullability(&patched, table_map)
            } else {
                restore_nullability(col, table_map)
            };
            let mut resolved = driver.resolve_param_type(&effective_col);

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

/// For INSERT queries, build a mapping from parameter number ($1, $2, ...) to the
/// corresponding column name in the INSERT column list.
///
/// Parses: `INSERT INTO table (col1, col2, col3) VALUES ($1, func(), $2::type, $3)`
/// Returns: {1 -> "col1", 2 -> "col3", 3 -> "col3"} (only entries for $N params)
///
/// This handles the case where sqlc strips column name/table info from cast params
/// (e.g., `$5::uuid` loses its `project_id` identity).
pub fn build_insert_param_map(sql: &str, insert_table: &Option<Identifier>) -> HashMap<i32, String> {
    let mut map = HashMap::new();

    // Only relevant for INSERT queries with a known target table
    if insert_table.is_none() {
        return map;
    }

    let sql_upper = sql.to_uppercase();
    let sql_norm = sql.replace('\n', " ");

    // Find the INSERT column list: everything between first (...) after INSERT INTO table
    let insert_pos = sql_upper.find("INSERT INTO");
    if insert_pos.is_none() {
        return map;
    }

    // Find the column list parentheses
    let after_insert = &sql_norm[insert_pos.unwrap()..];
    let col_open = match after_insert.find('(') {
        Some(p) => insert_pos.unwrap() + p,
        None => return map,
    };
    let col_close = match sql_norm[col_open..].find(')') {
        Some(p) => col_open + p,
        None => return map,
    };
    let col_list: Vec<&str> = sql_norm[col_open + 1..col_close]
        .split(',')
        .map(|s| s.trim())
        .collect();

    // Find the VALUES list
    let values_pos = sql_upper.find("VALUES");
    if values_pos.is_none() {
        return map;
    }
    let after_values = &sql_norm[values_pos.unwrap()..];
    let val_open = match after_values.find('(') {
        Some(p) => values_pos.unwrap() + p,
        None => return map,
    };
    // Handle nested parens in VALUES (e.g., gen_random_uuid())
    let mut depth = 0;
    let mut val_close = None;
    for (j, ch) in sql_norm[val_open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    val_close = Some(val_open + j);
                    break;
                }
            }
            _ => {}
        }
    }
    let val_close = match val_close {
        Some(p) => p,
        None => return map,
    };

    // Split values by comma, respecting nested parentheses
    let values_str = &sql_norm[val_open + 1..val_close];
    let mut values = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
    for ch in values_str.chars() {
        match ch {
            '(' => { paren_depth += 1; current.push(ch); }
            ')' => { paren_depth -= 1; current.push(ch); }
            ',' if paren_depth == 0 => {
                values.push(current.trim().to_string());
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        values.push(current.trim().to_string());
    }

    // Match each value expression to its column name
    if col_list.len() != values.len() {
        return map;
    }

    for (idx, val) in values.iter().enumerate() {
        // Extract $N from the value (may have ::cast suffix like "$5::uuid")
        let trimmed = val.trim();
        if trimmed.starts_with('$') {
            // Parse the number after $, stopping at non-digit (e.g., ::cast)
            let num_str: String = trimmed[1..].chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(num) = num_str.parse::<i32>() {
                map.insert(num, col_list[idx].to_string());
            }
        }
    }

    map
}

/// If a column has a source table reference, look up the original column definition
/// in the catalog to restore its nullability. This fixes the common case where
/// `nullable_col::text` causes sqlc to mark the result as NOT NULL.
///
/// When sqlc strips the table reference (e.g., through type casts), falls back to
/// searching all tables by column name. If exactly one match is found, uses its
/// nullability. If multiple matches exist, stays conservative (keeps not_null).
fn restore_nullability(col: &Column, table_map: &TableMap) -> Column {
    // If already nullable, nothing to restore
    if !col.not_null {
        return col.clone();
    }

    let col_name = if !col.original_name.is_empty() {
        &col.original_name
    } else {
        &col.name
    };

    // Try table reference first
    if let Some(ref table_ref) = col.table {
        if !table_ref.name.is_empty() {
            let table = table_map.get(&table_ref.name).or_else(|| {
                if !table_ref.schema.is_empty() {
                    table_map.get(&format!("{}.{}", table_ref.schema, table_ref.name))
                } else {
                    None
                }
            });
            if let Some(table) = table {
                if let Some(orig_col) = table.columns.iter().find(|c| c.name == *col_name) {
                    if !orig_col.not_null {
                        let mut fixed = col.clone();
                        fixed.not_null = false;
                        return fixed;
                    }
                }
            }
            return col.clone();
        }
    }

    // Fallback: no table reference (common with ::text casts).
    // Search all tables for a column with this name.
    let mut nullable_match = false;
    let mut match_count = 0;

    for table in table_map.values() {
        if let Some(orig_col) = table.columns.iter().find(|c| c.name == *col_name) {
            match_count += 1;
            if !orig_col.not_null {
                nullable_match = true;
            }
        }
    }

    // Only restore nullability if the column name is unambiguous (one table)
    // or ALL matches agree it's nullable.
    if match_count > 0 && nullable_match {
        let mut fixed = col.clone();
        fixed.not_null = false;
        return fixed;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::postgres::PostgresDriver;
    use crate::options::Options;
    use crate::plugin::plugin::{Column, Identifier, Parameter, Query, Table};

    /// Build a table map with a single table containing the given columns.
    fn make_table_map(table_name: &str, columns: Vec<Column>) -> TableMap {
        let mut map = TableMap::new();
        map.insert(
            table_name.to_string(),
            Table {
                rel: Some(Identifier {
                    catalog: String::new(),
                    schema: "public".into(),
                    name: table_name.into(),
                }),
                columns,
                comment: String::new(),
            },
        );
        map
    }

    fn make_column(name: &str, type_name: &str, not_null: bool) -> Column {
        Column {
            name: name.into(),
            not_null,
            r#type: Some(Identifier {
                catalog: String::new(),
                schema: String::new(),
                name: type_name.into(),
            }),
            table: Some(Identifier {
                catalog: String::new(),
                schema: "public".into(),
                name: String::new(), // will be set per-usage
            }),
            ..Default::default()
        }
    }

    fn make_param_column(name: &str, type_name: &str, not_null: bool, table_name: &str) -> Column {
        Column {
            name: name.into(),
            not_null,
            r#type: Some(Identifier {
                catalog: String::new(),
                schema: String::new(),
                name: type_name.into(),
            }),
            table: Some(Identifier {
                catalog: String::new(),
                schema: "public".into(),
                name: table_name.into(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_nullable_param_from_table_lookup() {
        // Schema: threads table with nullable project_id and task_id columns
        let table_columns = vec![
            make_column("id", "uuid", true),
            make_column("user_id", "text", true),
            make_column("title", "text", true),
            make_column("project_id", "uuid", false), // nullable
            make_column("task_id", "uuid", false),     // nullable
        ];
        let table_map = make_table_map("threads", table_columns);

        // Query: INSERT with params where sqlc marks them as not_null
        // even though the target columns are nullable
        let query = Query {
            text: "INSERT INTO threads (user_id, title, project_id, task_id) VALUES ($1, $2, $3::uuid, $4::uuid) RETURNING *".into(),
            name: "CreateThread".into(),
            cmd: ":one".into(),
            columns: vec![],
            params: vec![
                Parameter {
                    number: 1,
                    column: Some(make_param_column("user_id", "text", true, "threads")),
                },
                Parameter {
                    number: 2,
                    column: Some(make_param_column("title", "text", true, "threads")),
                },
                Parameter {
                    number: 3,
                    // sqlc marks this as not_null, but actual column is nullable
                    column: Some(make_param_column("project_id", "uuid", true, "threads")),
                },
                Parameter {
                    number: 4,
                    // sqlc marks this as not_null, but actual column is nullable
                    column: Some(make_param_column("task_id", "uuid", true, "threads")),
                },
            ],
            comments: vec![],
            filename: "threads.sql".into(),
            insert_into_table: None,
        };

        let options = Options {
            uuid_as_string: true,
            ..Default::default()
        };
        let driver = PostgresDriver::new(&options);

        let mut w = CodeWriter::new();
        generate_query_fn(&query, &table_map, &[], &mut w, &driver);
        let output = w.into_string();

        // The params type should have Option(String) for project_id and task_id
        assert!(
            output.contains("Option(String)"),
            "Expected Option(String) in params type for nullable columns, got:\n{output}"
        );

        // The param expressions should use nullable wrapping with value constructors
        assert!(
            output.contains("option.Some(v) -> Some(value.Text(v))  option.None -> None"),
            "Expected nullable param wrapping with value.Text, got:\n{output}"
        );

        // user_id and title should NOT be Option
        // Check the params type line specifically
        assert!(
            output.contains("user_id: String"),
            "Expected user_id: String (non-optional), got:\n{output}"
        );
        assert!(
            output.contains("title: String"),
            "Expected title: String (non-optional), got:\n{output}"
        );
        assert!(
            output.contains("project_id: Option(String)"),
            "Expected project_id: Option(String), got:\n{output}"
        );
        assert!(
            output.contains("task_id: Option(String)"),
            "Expected task_id: Option(String), got:\n{output}"
        );
    }

    #[test]
    fn test_not_null_params_unchanged() {
        // All columns are NOT NULL — params should remain non-optional
        let table_columns = vec![
            make_column("id", "uuid", true),
            make_column("name", "text", true),
            make_column("age", "int4", true),
        ];
        let table_map = make_table_map("users", table_columns);

        let query = Query {
            text: "INSERT INTO users (name, age) VALUES ($1, $2) RETURNING *".into(),
            name: "CreateUser".into(),
            cmd: ":one".into(),
            columns: vec![],
            params: vec![
                Parameter {
                    number: 1,
                    column: Some(make_param_column("name", "text", true, "users")),
                },
                Parameter {
                    number: 2,
                    column: Some(make_param_column("age", "int4", true, "users")),
                },
            ],
            comments: vec![],
            filename: "users.sql".into(),
            insert_into_table: None,
        };

        let options = Options::default();
        let driver = PostgresDriver::new(&options);

        let mut w = CodeWriter::new();
        generate_query_fn(&query, &table_map, &[], &mut w, &driver);
        let output = w.into_string();

        // No Option types should appear for non-nullable params
        assert!(
            !output.contains("Option("),
            "Expected no Option() types for NOT NULL params, got:\n{output}"
        );
        assert!(
            !output.contains("nullable("),
            "Expected no nullable() calls for NOT NULL params, got:\n{output}"
        );
    }

    #[test]
    fn test_single_nullable_param_no_params_type() {
        // Single param targeting a nullable column — no params struct generated,
        // the param appears directly in the function signature.
        let table_columns = vec![
            make_column("id", "int4", true),
            make_column("bio", "text", false), // nullable
        ];
        let table_map = make_table_map("authors", table_columns);

        let query = Query {
            text: "UPDATE authors SET bio = $1".into(),
            name: "UpdateBio".into(),
            cmd: ":exec".into(),
            columns: vec![],
            params: vec![Parameter {
                number: 1,
                // sqlc says not_null, but column is nullable
                column: Some(make_param_column("bio", "text", true, "authors")),
            }],
            comments: vec![],
            filename: "authors.sql".into(),
            insert_into_table: None,
        };

        let options = Options::default();
        let driver = PostgresDriver::new(&options);

        let mut w = CodeWriter::new();
        generate_query_fn(&query, &table_map, &[], &mut w, &driver);
        let output = w.into_string();

        // Single param should be in the function signature with Option type
        assert!(
            output.contains("bio: Option(String)"),
            "Expected bio: Option(String) in function signature, got:\n{output}"
        );
        assert!(
            output.contains("case bio { option.Some(v) -> Some(value.Text(v))  option.None -> None }"),
            "Expected nullable param wrapping with value.Text, got:\n{output}"
        );
    }

    #[test]
    fn test_insert_cast_params_recover_nullability() {
        // Mirrors the real sqlc behavior: for `$5::uuid` targeting a nullable column,
        // sqlc sends params with empty name, empty original_name, and no table reference.
        // The fix uses the INSERT column list to recover the column name.
        let table_columns = vec![
            make_column("id", "uuid", true),
            make_column("user_id", "text", true),
            make_column("title", "text", true),
            make_column("model", "text", false),       // nullable
            make_column("project_id", "uuid", false),  // nullable
            make_column("task_id", "uuid", false),      // nullable
        ];
        let table_map = make_table_map("threads", table_columns);

        // Simulate what sqlc actually sends: params 5 and 6 have empty name/table
        let query = Query {
            text: "INSERT INTO threads (id, user_id, title, model, project_id, task_id) VALUES (gen_random_uuid(), $1, $2, $3, $4::uuid, $5::uuid) RETURNING *".into(),
            name: "CreateThread".into(),
            cmd: ":one".into(),
            columns: vec![],
            params: vec![
                Parameter {
                    number: 1,
                    column: Some(make_param_column("user_id", "text", true, "threads")),
                },
                Parameter {
                    number: 2,
                    column: Some(make_param_column("title", "text", true, "threads")),
                },
                Parameter {
                    number: 3,
                    // model is nullable but sqlc provides the name, so restore_nullability handles it
                    column: Some(make_param_column("model", "text", false, "threads")),
                },
                Parameter {
                    number: 4,
                    // sqlc strips name and table for cast params
                    column: Some(Column {
                        name: String::new(),
                        original_name: String::new(),
                        not_null: true,
                        r#type: Some(Identifier {
                            catalog: String::new(),
                            schema: String::new(),
                            name: "uuid".into(),
                        }),
                        table: None,
                        ..Default::default()
                    }),
                },
                Parameter {
                    number: 5,
                    column: Some(Column {
                        name: String::new(),
                        original_name: String::new(),
                        not_null: true,
                        r#type: Some(Identifier {
                            catalog: String::new(),
                            schema: String::new(),
                            name: "uuid".into(),
                        }),
                        table: None,
                        ..Default::default()
                    }),
                },
            ],
            comments: vec![],
            filename: "threads.sql".into(),
            insert_into_table: Some(Identifier {
                catalog: String::new(),
                schema: String::new(),
                name: "threads".into(),
            }),
        };

        let options = Options {
            uuid_as_string: true,
            ..Default::default()
        };
        let driver = PostgresDriver::new(&options);

        let mut w = CodeWriter::new();
        generate_query_fn(&query, &table_map, &[], &mut w, &driver);
        let output = w.into_string();

        // project_id and task_id should be recovered as Option(String)
        assert!(
            output.contains("project_id: Option(String)"),
            "Expected project_id: Option(String) recovered from INSERT mapping, got:\n{output}"
        );
        assert!(
            output.contains("task_id: Option(String)"),
            "Expected task_id: Option(String) recovered from INSERT mapping, got:\n{output}"
        );

        // Should use nullable wrapping for these params
        assert!(
            output.contains("option.Some(v) -> Some(value.Text(v))  option.None -> None"),
            "Expected nullable param wrapping with value.Text, got:\n{output}"
        );

        // user_id and title should remain non-optional
        assert!(
            output.contains("user_id: String"),
            "Expected user_id: String (non-optional), got:\n{output}"
        );
        assert!(
            output.contains("title: String"),
            "Expected title: String (non-optional), got:\n{output}"
        );

        // model should already be Option (sqlc provided it as not_null=false)
        assert!(
            output.contains("model: Option(String)"),
            "Expected model: Option(String), got:\n{output}"
        );
    }
}
