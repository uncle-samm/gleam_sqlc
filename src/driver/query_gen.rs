use std::collections::{HashMap, HashSet};

use crate::codegen::TableMap;
use crate::driver::traits::Driver;
use crate::driver::type_map::{ResolvedType, TypeContext};
use crate::generators::naming::{
    column_to_field_name, doc_comment_lines, escape_gleam_string, query_to_fn_name, to_pascal_case,
};
use crate::options::TypeOverride;
use crate::plugin::plugin::{Column, Identifier, Parameter, Query};
use crate::utils::CodeWriter;

struct ParamInfo {
    /// Field name in the Params type (or the argument name when there is one param).
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

/// Local names used in generated function bodies. A lone parameter with one of
/// these names is renamed so it cannot be shadowed.
const LOCAL_NAMES: &[&str] = &[
    "conn",
    "decoder",
    "sql",
    "params",
    "param_sets",
    "rows",
    "response",
];

/// sqlc commands this plugin generates code for.
pub const SUPPORTED_COMMANDS: &[&str] = &[
    ":one",
    ":many",
    ":exec",
    ":execrows",
    ":execlastid",
    ":copyfrom",
];

/// Generate a Gleam function for a single query.
pub fn generate_query_fn(
    query: &Query,
    table_map: &TableMap,
    overrides: &[TypeOverride],
    ctx: &TypeContext,
    w: &mut CodeWriter,
    driver: &dyn Driver,
) -> Result<(), String> {
    let cmd = query.cmd.as_str();
    if !SUPPORTED_COMMANDS.contains(&cmd) {
        return Err(format!(
            "query '{}' uses '{cmd}', which sqlc-gen-gleam does not support (supported: {})",
            query.name,
            SUPPORTED_COMMANDS.join(" ")
        ));
    }
    let fn_name = query_to_fn_name(&query.name);
    let params = resolve_params(query, table_map, ctx, driver)?;
    let columns = resolve_columns(
        &query.columns,
        table_map,
        &query.name,
        overrides,
        ctx,
        driver,
    );
    let has_slices = params.iter().any(|p| p.is_slice);
    // Only queries with a slice change shape at run time.
    let positional = if has_slices {
        positional_order(&query.text, &query.params, &params)
    } else {
        None
    };
    let (sql_text, ordered): (&str, Vec<&ParamInfo>) = match &positional {
        Some((text, order)) => (text, order.iter().map(|i| &params[*i]).collect()),
        None => (&query.text, params.iter().collect()),
    };

    let const_name = format!("{fn_name}_sql");
    w.writef(format_args!(
        "const {const_name} = \"{}\"",
        escape_gleam_string(sql_text)
    ));
    w.blank();

    let base_type_name = to_pascal_case(&fn_name);
    let row_type_name = format!("{base_type_name}Row");
    let returns_rows = !columns.is_empty() && matches!(cmd, ":one" | ":many" | ":execlastid");
    if returns_rows {
        generate_row_type(&row_type_name, &columns, w);
        w.blank();
    }

    let params_type_name = format!("{base_type_name}Params");
    let use_params_type = cmd == ":copyfrom" || params.len() >= 2;
    if use_params_type && !params.is_empty() {
        generate_params_type(&params_type_name, &params, w);
        w.blank();
    }

    for line in query
        .comments
        .iter()
        .flat_map(|c| doc_comment_lines(c.trim()))
    {
        w.line(&line);
    }
    let sig = if cmd == ":copyfrom" {
        format!("{fn_name}(conn, rows: List({params_type_name}))")
    } else {
        build_fn_signature(&fn_name, &params, &params_type_name)
    };
    w.writef(format_args!("pub fn {sig} {{"));
    w.indent();

    if cmd == ":copyfrom" {
        generate_copyfrom_body(&const_name, &ordered, w, driver);
    } else {
        let var_of = |p: &ParamInfo| {
            if use_params_type {
                format!("params.{}", p.name)
            } else {
                p.name.clone()
            }
        };
        let sql_var = if has_slices {
            emit_slice_sql(&const_name, &params, &var_of, w);
            "sql".to_string()
        } else {
            const_name.clone()
        };
        let params_str = emit_params(&ordered, &var_of, w, driver);
        if returns_rows {
            generate_decoder(&columns, &row_type_name, w);
        }
        match cmd {
            ":one" => driver.write_one_call(&sql_var, &params_str, w),
            ":many" => driver.write_many_call(&sql_var, &params_str, w),
            ":exec" => driver.write_exec_call(&sql_var, &params_str, w),
            ":execrows" => driver.write_execrows_call(&sql_var, &params_str, w),
            ":execlastid" if returns_rows => driver.write_one_call(&sql_var, &params_str, w),
            ":execlastid" => driver.write_exec_call(&sql_var, &params_str, w),
            other => unreachable!("unsupported command {other} passed the check above"),
        }
    }

    w.dedent();
    w.line("}");
    Ok(())
}

fn resolve_params(
    query: &Query,
    table_map: &TableMap,
    ctx: &TypeContext,
    driver: &dyn Driver,
) -> Result<Vec<ParamInfo>, String> {
    // Extract slice marker names from the SQL text: /*SLICE:xxx*/?
    let slice_markers = extract_slice_markers(&query.text);
    let mut slice_idx = 0;

    // For INSERT queries where sqlc strips column names from cast params (e.g. $5::uuid),
    // build a mapping from $N -> column name by parsing the INSERT column list and VALUES.
    let insert_param_names = build_insert_param_map(&query.text, &query.insert_into_table);
    let mut seen_names: HashMap<String, usize> = HashMap::new();
    let single = query.params.len() == 1 && query.cmd != ":copyfrom";

    let mut result = Vec::with_capacity(query.params.len());
    for (i, p) in query.params.iter().enumerate() {
        let col = p.column.as_ref().ok_or_else(|| {
            format!(
                "query '{}': parameter ${} has no column information",
                query.name, p.number
            )
        })?;
        let is_slice = col.is_sqlc_slice;

        // The INSERT column this parameter fills, for when sqlc lost track
        // of it (a cast such as `$5::uuid` drops the column and its table).
        let has_table = col.table.as_ref().is_some_and(|t| !t.name.is_empty());
        let insert_col = if has_table {
            None
        } else {
            insert_param_names.get(&p.number)
        };

        // sqlc.arg(), sqlc.narg() and sqlc.slice() put the name the query
        // author chose in `name`; `original_name` is the column the
        // parameter is used with.
        let raw_name = if !col.name.is_empty() {
            column_to_field_name(&col.name)
        } else if !col.original_name.is_empty() {
            column_to_field_name(&col.original_name)
        } else if let Some(name) = insert_col {
            column_to_field_name(name)
        } else {
            format!("param_{}", i + 1)
        };
        let mut name = deduplicate_name(&raw_name, &mut seen_names);
        if single && LOCAL_NAMES.contains(&name.as_str()) {
            name.push_str("_value");
        }

        let slice_marker = if is_slice {
            let marker = slice_markers
                .get(slice_idx)
                .cloned()
                .unwrap_or_else(|| raw_name.clone());
            slice_idx += 1;
            marker
        } else {
            String::new()
        };

        // Restore nullability from the column the parameter belongs to: the
        // one sqlc points at, or the INSERT target column. A parameter is
        // never matched to a column by its name alone, since the name is the
        // query author's choice (use sqlc.narg() for a nullable parameter).
        let effective_col = match insert_col {
            Some(column) => {
                let mut patched = col.clone();
                patched.name = column.clone();
                patched.original_name = column.clone();
                patched.table = query.insert_into_table.clone();
                restore_nullability(&patched, table_map, false)
            }
            None => restore_nullability(col, table_map, false),
        };
        let mut resolved = driver.resolve_column_type(&effective_col, ctx);
        if is_slice {
            resolved.type_expr = format!("List({})", resolved.type_expr);
        }

        result.push(ParamInfo {
            name,
            resolved,
            is_slice,
            slice_marker,
        });
    }
    Ok(result)
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

fn resolve_columns(
    columns: &[Column],
    table_map: &TableMap,
    query_name: &str,
    overrides: &[TypeOverride],
    ctx: &TypeContext,
    driver: &dyn Driver,
) -> Vec<ColumnInfo> {
    let mut result = Vec::new();
    let mut seen_names: HashMap<String, usize> = HashMap::new();

    for col in columns {
        let embed = col.embed_table.as_ref().filter(|t| !t.name.is_empty());
        if let Some(embed) = embed {
            let prefix = if !col.table_alias.is_empty() {
                column_to_field_name(&col.table_alias)
            } else {
                column_to_field_name(&embed.name)
            };
            let table = table_map.get(&embed.name).or_else(|| {
                (!embed.schema.is_empty())
                    .then(|| table_map.get(&format!("{}.{}", embed.schema, embed.name)))
                    .flatten()
            });
            match table {
                // Expand the embedded table into prefixed fields.
                Some(table) => {
                    for table_col in &table.columns {
                        let base_name =
                            format!("{prefix}_{}", column_to_field_name(&table_col.name));
                        result.push(ColumnInfo {
                            field_name: deduplicate_name(&base_name, &mut seen_names),
                            resolved: driver.resolve_column_type(table_col, ctx),
                        });
                    }
                }
                None => {
                    eprintln!(
                        "warning: sqlc.embed({}) table not found in catalog; treating it as one column",
                        embed.name
                    );
                    result.push(ColumnInfo {
                        field_name: deduplicate_name(&prefix, &mut seen_names),
                        resolved: driver.resolve_column_type(col, ctx),
                    });
                }
            }
        } else {
            let field_name = deduplicate_name(&column_to_field_name(&col.name), &mut seen_names);
            let resolved = match find_override(overrides, query_name, &col.name) {
                Some(o) => driver.resolve_override(o, ctx),
                // Restore nullability from the source table column when sqlc loses it
                // through type casts (e.g., `nullable_col::text`).
                None => driver.resolve_column_type(&restore_nullability(col, table_map, true), ctx),
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
pub fn build_insert_param_map(
    sql: &str,
    insert_table: &Option<Identifier>,
) -> HashMap<i32, String> {
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
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
            }
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
        if let Some(rest) = trimmed.strip_prefix('$') {
            // Parse the number after $, stopping at non-digit (e.g., ::cast)
            let num_str: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(num) = num_str.parse::<i32>() {
                map.insert(num, col_list[idx].to_string());
            }
        }
    }

    map
}

/// sqlc can lose a column's nullability, e.g. `nullable_col::text` is reported as
/// NOT NULL. If the column still points at its table, use the table's definition.
///
/// When sqlc dropped the table reference and `search_catalog` is set, fall back
/// to the catalog only if exactly one table has a column with that name. With
/// several candidates we cannot know which one is meant, so sqlc's answer is kept.
fn restore_nullability(col: &Column, table_map: &TableMap, search_catalog: bool) -> Column {
    if !col.not_null {
        return col.clone();
    }
    let col_name = if !col.original_name.is_empty() {
        &col.original_name
    } else {
        &col.name
    };
    if col_name.is_empty() {
        return col.clone();
    }
    let nullable_in = |table: &crate::plugin::plugin::Table| {
        table
            .columns
            .iter()
            .find(|c| c.name == *col_name)
            .map(|c| !c.not_null)
    };

    let source_nullable = match col.table.as_ref().filter(|t| !t.name.is_empty()) {
        Some(table_ref) => lookup_table(table_map, table_ref).and_then(nullable_in),
        None if !search_catalog => None,
        None => {
            // table_map holds each table under its plain and its qualified name;
            // count distinct tables only.
            let mut seen = HashSet::new();
            let matches: Vec<bool> = table_map
                .values()
                .filter(|t| seen.insert(t.rel.as_ref().map(|r| (r.schema.clone(), r.name.clone()))))
                .filter_map(nullable_in)
                .collect();
            match matches.as_slice() {
                [only] => Some(*only),
                _ => None,
            }
        }
    };

    let mut fixed = col.clone();
    if source_nullable == Some(true) {
        fixed.not_null = false;
    }
    fixed
}

fn lookup_table<'a>(
    table_map: &'a TableMap,
    id: &Identifier,
) -> Option<&'a crate::plugin::plugin::Table> {
    table_map.get(&id.name).or_else(|| {
        (!id.schema.is_empty())
            .then(|| table_map.get(&format!("{}.{}", id.schema, id.name)))
            .flatten()
    })
}

/// Find the type override for a query column. An override naming the query
/// wins over a `*` one.
fn find_override<'a>(
    overrides: &'a [TypeOverride],
    query_name: &str,
    column_name: &str,
) -> Option<&'a TypeOverride> {
    let matching = |wanted: &str| {
        overrides.iter().find(|o| {
            o.column
                .split_once(':')
                .is_some_and(|(qname, cname)| qname == wanted && cname == column_name)
        })
    };
    matching(query_name).or_else(|| matching("*"))
}

/// A `?` placeholder in SQLite SQL text.
#[derive(Debug, PartialEq)]
enum Placeholder {
    /// `?`: SQLite numbers it one above the highest number so far.
    Bare,
    /// `?N`, which sqlc writes for sqlc.arg() and sqlc.narg().
    Numbered(i32),
    /// `/*SLICE:name*/?`, which sqlc writes for sqlc.slice(name).
    Slice(String),
}

/// Find the `?` placeholders outside strings, quoted identifiers and comments,
/// each with the byte range of its `?` or `?N`.
fn scan_placeholders(sql: &str) -> Vec<(std::ops::Range<usize>, Placeholder)> {
    let bytes = sql.as_bytes();
    let find = |from: usize, needle: &str| sql[from..].find(needle).map(|p| from + p);
    let mut found = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'\'' | b'"' | b'`') => {
                // A doubled quote is an escaped quote: it ends one string
                // and starts the next, which skips it just the same.
                i += 1;
                while i < bytes.len() && bytes[i] != quote {
                    i += 1;
                }
                i += 1;
            }
            b'[' => i = find(i, "]").map_or(bytes.len(), |end| end + 1),
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = find(i, "\n").map_or(bytes.len(), |end| end + 1)
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let Some(end) = find(i + 2, "*/") else {
                    break;
                };
                let after = end + 2;
                match sql[i + 2..end].strip_prefix("SLICE:") {
                    Some(name) if bytes.get(after) == Some(&b'?') => {
                        found.push((after..after + 1, Placeholder::Slice(name.to_string())));
                        i = after + 1;
                    }
                    _ => i = after,
                }
            }
            b'?' => {
                let digits = bytes[i + 1..]
                    .iter()
                    .take_while(|b| b.is_ascii_digit())
                    .count();
                let end = i + 1 + digits;
                let placeholder = match sql[i + 1..end].parse::<i32>() {
                    Ok(n) => Placeholder::Numbered(n),
                    Err(_) => Placeholder::Bare,
                };
                found.push((i..end, placeholder));
                i = end;
            }
            _ => i += 1,
        }
    }
    found
}

/// A query that mixes numbered parameters (`?1`, from sqlc.arg) with
/// sqlc.slice cannot run as written: the slice becomes one bare `?` per item,
/// SQLite numbers each bare `?` one above the highest number so far, and
/// those numbers then collide with the `?N` after it.
///
/// For such a query, return its SQL with every `?N` turned into a bare `?`,
/// and the indexes of `params` in order of appearance (one that appears
/// twice is listed twice), so every parameter is bound by position.
/// `None` when the query has no numbered parameter, or when a placeholder
/// cannot be matched to a parameter; the SQL is then used as sqlc wrote it.
fn positional_order(
    sql: &str,
    query_params: &[Parameter],
    params: &[ParamInfo],
) -> Option<(String, Vec<usize>)> {
    let placeholders = scan_placeholders(sql);
    if !placeholders
        .iter()
        .any(|(_, p)| matches!(p, Placeholder::Numbered(_)))
    {
        return None;
    }
    let by_number = |n: i32| query_params.iter().position(|p| p.number == n);

    let mut text = String::with_capacity(sql.len());
    let mut order = Vec::with_capacity(placeholders.len());
    let mut copied = 0;
    let mut highest = 0;
    for (range, placeholder) in placeholders {
        let index = match placeholder {
            Placeholder::Numbered(n) => {
                highest = highest.max(n);
                by_number(n)?
            }
            Placeholder::Slice(name) => {
                highest += 1;
                params
                    .iter()
                    .position(|p| p.is_slice && p.slice_marker == name)?
            }
            Placeholder::Bare => {
                highest += 1;
                by_number(highest)?
            }
        };
        order.push(index);
        text.push_str(&sql[copied..range.start]);
        text.push('?');
        copied = range.end;
    }
    text.push_str(&sql[copied..]);
    Some((text, order))
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
    match params {
        [] => format!("{fn_name}(conn)"),
        [p] => format!("{fn_name}(conn, {}: {})", p.name, p.resolved.type_expr),
        _ => format!("{fn_name}(conn, params: {params_type_name})"),
    }
}

/// Decoder with positional variable names, so column names can never shadow
/// `decode` or other names used in the body.
fn generate_decoder(columns: &[ColumnInfo], row_type_name: &str, w: &mut CodeWriter) {
    w.line("let decoder = {");
    w.indent();
    for (i, col) in columns.iter().enumerate() {
        w.writef(format_args!(
            "use c{i} <- decode.element({i}, {})",
            col.resolved.decoder_expr.to_gleam()
        ));
    }
    let fields: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{}: c{i}", c.field_name))
        .collect();
    w.writef(format_args!(
        "decode.success({row_type_name}({}))",
        fields.join(", ")
    ));
    w.dedent();
    w.line("}");
}

/// Emit `use x_param <- result.try(...)` lines for params that must be prepared
/// (e.g. UUID strings) and return the params list expression.
fn emit_params(
    params: &[&ParamInfo],
    var_of: &dyn Fn(&ParamInfo) -> String,
    w: &mut CodeWriter,
    driver: &dyn Driver,
) -> String {
    if params.is_empty() {
        return "[]".into();
    }
    let has_slices = params.iter().any(|p| p.is_slice);
    let mut parts = Vec::with_capacity(params.len());
    for p in params.iter().copied() {
        let var = var_of(p);
        let rendered = if p.is_slice {
            driver.render_slice_param(&p.resolved.param_expr, &var)
        } else {
            let value = match driver.prepare_param(&p.resolved.param_expr, &var) {
                Some(prep) => {
                    let bound = format!("{}_param", p.name);
                    w.writef(format_args!("use {bound} <- result.try({prep})"));
                    driver.render_param(&p.resolved.param_expr, &bound)
                }
                None => driver.render_param(&p.resolved.param_expr, &var),
            };
            if has_slices {
                format!("[{value}]")
            } else {
                value
            }
        };
        parts.push(rendered);
    }
    if has_slices {
        format!("list.flatten([{}])", parts.join(", "))
    } else {
        format!("[{}]", parts.join(", "))
    }
}

/// Emit `let sql = ...` replacing each /*SLICE:name*/? marker with one `?` per item.
fn emit_slice_sql(
    const_name: &str,
    params: &[ParamInfo],
    var_of: &dyn Fn(&ParamInfo) -> String,
    w: &mut CodeWriter,
) {
    let mut current = const_name.to_string();
    for sp in params.iter().filter(|p| p.is_slice) {
        let var = var_of(sp);
        w.writef(format_args!(
            "let sql = string.replace({current}, \"/*SLICE:{}*/?\", string.join(list.repeat(\"?\", list.length({var})), \", \"))",
            escape_gleam_string(&sp.slice_marker)
        ));
        current = "sql".to_string();
    }
}

fn generate_copyfrom_body(
    const_name: &str,
    params: &[&ParamInfo],
    w: &mut CodeWriter,
    driver: &dyn Driver,
) {
    let needs_prep = params.iter().any(|p| {
        driver
            .prepare_param(&p.resolved.param_expr, "row")
            .is_some()
    });
    if needs_prep {
        w.line("use param_sets <- result.try(list.try_map(rows, fn(row) {");
        w.indent();
        let list = emit_params(
            params,
            &|p: &ParamInfo| format!("row.{}", p.name),
            w,
            driver,
        );
        w.writef(format_args!("Ok({list})"));
        w.dedent();
        w.line("}))");
    } else {
        w.line("let param_sets = list.map(rows, fn(row) {");
        w.indent();
        let list = emit_params(
            params,
            &|p: &ParamInfo| format!("row.{}", p.name),
            w,
            driver,
        );
        w.line(&list);
        w.dedent();
        w.line("})");
    }
    driver.write_copyfrom_call(const_name, "param_sets", w);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::postgres::PostgresDriver;
    use crate::driver::traits::Driver;
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
            make_column("task_id", "uuid", false),    // nullable
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
        let enums = crate::driver::type_map::EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        generate_query_fn(&query, &table_map, &[], &ctx, &mut w, &driver).unwrap();
        let output = w.into_string();

        // The params type should have Option(String) for project_id and task_id
        assert!(
            output.contains("Option(String)"),
            "Expected Option(String) in params type for nullable columns, got:\n{output}"
        );

        // The param expressions should use nullable wrapping with value constructors
        assert!(
            output.contains("case params.project_id { option.Some(u) -> uuid_param(u) |> result.map(Some)  option.None -> Ok(None) }"),
            "Expected nullable UUID strings to be parsed, got:\n{output}"
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
        let enums = crate::driver::type_map::EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        generate_query_fn(&query, &table_map, &[], &ctx, &mut w, &driver).unwrap();
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
        let enums = crate::driver::type_map::EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        generate_query_fn(&query, &table_map, &[], &ctx, &mut w, &driver).unwrap();
        let output = w.into_string();

        // Single param should be in the function signature with Option type
        assert!(
            output.contains("bio: Option(String)"),
            "Expected bio: Option(String) in function signature, got:\n{output}"
        );
        assert!(
            output.contains("[option.map(bio, value.Text)]"),
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
            make_column("model", "text", false),      // nullable
            make_column("project_id", "uuid", false), // nullable
            make_column("task_id", "uuid", false),    // nullable
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
        let enums = crate::driver::type_map::EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        generate_query_fn(&query, &table_map, &[], &ctx, &mut w, &driver).unwrap();
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
            output.contains("case params.project_id { option.Some(u) -> uuid_param(u) |> result.map(Some)  option.None -> Ok(None) }"),
            "Expected nullable UUID strings to be parsed, got:\n{output}"
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

    fn render(query: &Query, table_map: &TableMap, driver: &dyn Driver) -> Result<String, String> {
        let enums = crate::driver::type_map::EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        let mut w = CodeWriter::new();
        generate_query_fn(query, table_map, &[], &ctx, &mut w, driver)?;
        Ok(w.into_string())
    }

    fn simple_query(
        name: &str,
        cmd: &str,
        text: &str,
        columns: Vec<Column>,
        params: Vec<Parameter>,
    ) -> Query {
        Query {
            text: text.into(),
            name: name.into(),
            cmd: cmd.into(),
            columns,
            params,
            comments: vec![],
            filename: "q.sql".into(),
            insert_into_table: None,
        }
    }

    #[test]
    fn test_ambiguous_column_name_keeps_sqlc_nullability() {
        // users.email is NOT NULL, profiles.email is nullable. A cast drops the
        // table reference; with two candidates the plugin must not guess.
        let mut table_map = make_table_map("users", vec![make_column("email", "text", true)]);
        table_map.extend(make_table_map(
            "profiles",
            vec![make_column("email", "text", false)],
        ));
        let mut c = make_column("email", "text", true);
        c.table = None;
        let q = simple_query(
            "GetEmail",
            ":many",
            "SELECT email::text AS email FROM users",
            vec![c.clone()],
            vec![],
        );
        let out = render(&q, &table_map, &PostgresDriver::new(&Options::default())).unwrap();
        assert!(out.contains("GetEmailRow(email: String)"), "{out}");

        // With a single candidate the catalog wins.
        let table_map = make_table_map("profiles", vec![make_column("email", "text", false)]);
        let out = render(&q, &table_map, &PostgresDriver::new(&Options::default())).unwrap();
        assert!(out.contains("GetEmailRow(email: Option(String))"), "{out}");
    }

    #[test]
    fn test_unsupported_command_is_an_error() {
        let q = simple_query("Batch", ":batchexec", "SELECT 1", vec![], vec![]);
        let err = render(
            &q,
            &TableMap::new(),
            &PostgresDriver::new(&Options::default()),
        )
        .unwrap_err();
        assert!(err.contains(":batchexec"), "{err}");
    }

    #[test]
    fn test_lone_param_named_like_a_local_is_renamed() {
        let table_map = make_table_map("t", vec![make_column("decoder", "int4", true)]);
        let q = simple_query(
            "GetByDecoder",
            ":one",
            "SELECT decoder FROM t WHERE decoder = $1",
            vec![make_param_column("decoder", "int4", true, "t")],
            vec![Parameter {
                number: 1,
                column: Some(make_param_column("decoder", "int4", true, "t")),
            }],
        );
        let out = render(&q, &table_map, &PostgresDriver::new(&Options::default())).unwrap();
        assert!(
            out.contains("pub fn get_by_decoder(conn, decoder_value: Int)"),
            "{out}"
        );
        assert!(
            out.contains("use c0 <- decode.element(0, decode.int)"),
            "{out}"
        );
        assert!(out.contains("GetByDecoderRow(decoder: c0)"), "{out}");
    }

    #[test]
    fn test_copyfrom_postgres_uses_query_batch() {
        let table_map = make_table_map(
            "t",
            vec![
                make_column("a", "text", true),
                make_column("b", "int4", false),
            ],
        );
        let q = simple_query(
            "InsertMany",
            ":copyfrom",
            "INSERT INTO t (a, b) VALUES ($1, $2)",
            vec![],
            vec![
                Parameter {
                    number: 1,
                    column: Some(make_param_column("a", "text", true, "t")),
                },
                Parameter {
                    number: 2,
                    column: Some(make_param_column("b", "int4", false, "t")),
                },
            ],
        );
        let out = render(&q, &table_map, &PostgresDriver::new(&Options::default())).unwrap();
        assert!(
            out.contains("let param_sets = list.map(rows, fn(row) {"),
            "{out}"
        );
        assert!(
            out.contains("[Some(value.Text(row.a)), option.map(row.b, value.Integer)]"),
            "{out}"
        );
        assert!(
            out.contains("postgleam.query_batch(conn, insert_many_sql, param_sets)"),
            "{out}"
        );
    }

    #[test]
    fn test_sqlite_params_are_not_wrapped() {
        use crate::driver::sqlite::SqliteDriver;
        let table_map = make_table_map(
            "t",
            vec![
                make_column("a", "text", true),
                make_column("b", "integer", false),
            ],
        );
        let q = simple_query(
            "Insert",
            ":exec",
            "INSERT INTO t (a, b) VALUES (?, ?)",
            vec![],
            vec![
                Parameter {
                    number: 1,
                    column: Some(make_param_column("a", "text", true, "t")),
                },
                Parameter {
                    number: 2,
                    column: Some(make_param_column("b", "integer", false, "t")),
                },
            ],
        );
        let out = render(&q, &table_map, &SqliteDriver::new(&Options::default())).unwrap();
        assert!(out.contains("glite.exec(conn, insert_sql, [glite.text(params.a), glite.nullable(params.b, glite.int)])"), "{out}");
        assert!(!out.contains("Some("), "{out}");
    }

    fn named(name: &str, original: &str, type_name: &str, not_null: bool, table: &str) -> Column {
        Column {
            original_name: original.into(),
            is_named_param: true,
            ..make_param_column(name, type_name, not_null, table)
        }
    }

    #[test]
    fn test_params_are_named_after_sqlc_arg() {
        // WHERE age >= sqlc.arg(min_age) AND age <= sqlc.arg(max_age):
        // sqlc reports the name in `name` and the column in `original_name`.
        let table_map = make_table_map("people", vec![make_column("age", "int4", false)]);
        let q = simple_query(
            "ByAge",
            ":many",
            "SELECT 1 FROM people WHERE age >= $1 AND age <= $2",
            vec![],
            vec![
                Parameter {
                    number: 1,
                    column: Some(named("min_age", "age", "int4", true, "people")),
                },
                Parameter {
                    number: 2,
                    column: Some(named("max_age", "age", "int4", true, "people")),
                },
            ],
        );
        let out = render(&q, &table_map, &PostgresDriver::new(&Options::default())).unwrap();
        // Nullability still comes from the column the parameter is used with.
        assert!(
            out.contains("ByAgeParams(min_age: Option(Int), max_age: Option(Int))"),
            "{out}"
        );
    }

    #[test]
    fn test_param_is_not_matched_to_a_column_by_name() {
        // sqlc.arg(bio)::text has no table. A table that happens to have a
        // nullable `bio` column must not make the parameter optional.
        let table_map = make_table_map("authors", vec![make_column("bio", "text", false)]);
        let mut column = named("bio", "", "text", true, "");
        column.table = None;
        let q = simple_query(
            "Search",
            ":many",
            "SELECT 1 WHERE $1::text <> ''",
            vec![],
            vec![Parameter {
                number: 1,
                column: Some(column.clone()),
            }],
        );
        let driver = PostgresDriver::new(&Options::default());
        let out = render(&q, &table_map, &driver).unwrap();
        assert!(out.contains("pub fn search(conn, bio: String)"), "{out}");

        // In an INSERT the target column decides, whatever the name.
        column.name = "biography".into();
        let mut q = simple_query(
            "Add",
            ":exec",
            "INSERT INTO authors (bio) VALUES ($1::text)",
            vec![],
            vec![Parameter {
                number: 1,
                column: Some(column),
            }],
        );
        q.insert_into_table = Some(Identifier {
            name: "authors".into(),
            ..Default::default()
        });
        let out = render(&q, &table_map, &driver).unwrap();
        assert!(
            out.contains("pub fn add(conn, biography: Option(String))"),
            "{out}"
        );
    }

    #[test]
    fn test_scan_placeholders_skips_strings_and_comments() {
        let sql = "SELECT '?1', \"a?\", `b?`, [c?] -- ?2\n/* ?3 */ FROM t WHERE a = ?4 AND b IN (/*SLICE:ids*/?) AND c = ? AND d = 'it''s ?'";
        let found: Vec<Placeholder> = scan_placeholders(sql).into_iter().map(|(_, p)| p).collect();
        assert_eq!(
            found,
            vec![
                Placeholder::Numbered(4),
                Placeholder::Slice("ids".into()),
                Placeholder::Bare
            ]
        );
    }

    fn sqlite_param(number: i32, name: &str, type_name: &str, slice: bool) -> Parameter {
        Parameter {
            number,
            column: Some(Column {
                is_sqlc_slice: slice,
                ..make_param_column(name, type_name, true, "t")
            }),
        }
    }

    #[test]
    fn test_numbered_params_with_a_slice_are_bound_by_position() {
        use crate::driver::sqlite::SqliteDriver;
        let table_map = make_table_map(
            "t",
            vec![
                make_column("id", "integer", true),
                make_column("name", "text", true),
            ],
        );
        // sqlc.arg(name) twice around a sqlc.slice(ids): expanded as written,
        // SQLite would number the slice's `?`s 2, 3, ... and clash with ?3.
        let q = simple_query(
            "Find",
            ":many",
            "SELECT id FROM t WHERE name = ?1 AND id IN (/*SLICE:ids*/?) AND id > ?3 AND name <> ?1",
            vec![],
            vec![
                sqlite_param(1, "name", "text", false),
                sqlite_param(2, "ids", "integer", true),
                sqlite_param(3, "min_id", "integer", false),
            ],
        );
        let out = render(&q, &table_map, &SqliteDriver::new(&Options::default())).unwrap();
        assert!(
            out.contains(
                "const find_sql = \"SELECT id FROM t WHERE name = ? AND id IN (/*SLICE:ids*/?) AND id > ? AND name <> ?\""
            ),
            "{out}"
        );
        assert!(
            out.contains(
                "list.flatten([[glite.text(params.name)], list.map(params.ids, glite.int), [glite.int(params.min_id)], [glite.text(params.name)]])"
            ),
            "{out}"
        );

        // Without numbered parameters the SQL is used as sqlc wrote it.
        let q = simple_query(
            "Plain",
            ":many",
            "SELECT id FROM t WHERE name = ? AND id IN (/*SLICE:ids*/?)",
            vec![],
            vec![
                sqlite_param(1, "name", "text", false),
                sqlite_param(2, "ids", "integer", true),
            ],
        );
        let out = render(&q, &table_map, &SqliteDriver::new(&Options::default())).unwrap();
        assert!(
            out.contains("name = ? AND id IN (/*SLICE:ids*/?)\""),
            "{out}"
        );
        assert!(
            out.contains(
                "list.flatten([[glite.text(params.name)], list.map(params.ids, glite.int)])"
            ),
            "{out}"
        );
    }

    #[test]
    fn test_overrides() {
        let overrides: Vec<TypeOverride> = serde_json::from_str(
            r#"[
                {"column": "*:total", "gleamType": {"type": "Int", "notNull": true}},
                {"column": "Stats:total", "dbType": {"type": "numeric"}},
                {"column": "Stats:ids", "dbType": {"type": "int8", "array": true, "notNull": true}}
            ]"#,
        )
        .unwrap();
        let mut total = make_column("total", "bigint", true);
        total.table = None;
        let mut ids = make_column("ids", "anyarray", true);
        ids.table = None;
        let driver = PostgresDriver::new(&Options::default());
        let enums = crate::driver::type_map::EnumMap::new();
        let ctx = TypeContext::without_enums(&enums);
        let render = |name: &str| {
            let q = simple_query(
                name,
                ":one",
                "SELECT 1",
                vec![total.clone(), ids.clone()],
                vec![],
            );
            let mut w = CodeWriter::new();
            generate_query_fn(&q, &TableMap::new(), &overrides, &ctx, &mut w, &driver).unwrap();
            w.into_string()
        };
        // The override naming the query wins over the `*` one.
        let out = render("Stats");
        assert!(
            out.contains("StatsRow(total: Option(String), ids: List(Int))"),
            "{out}"
        );
        assert!(
            out.contains("decode.element(0, decode.optional(decode.numeric))"),
            "{out}"
        );
        assert!(
            out.contains("decode.element(1, decode_array(decode.int))"),
            "{out}"
        );
        let out = render("Other");
        assert!(out.contains("OtherRow(total: Int, ids: String)"), "{out}");
    }

    #[test]
    fn test_query_comments_become_docs() {
        let mut q = simple_query("Ping", ":exec", "SELECT 1", vec![], vec![]);
        q.comments = vec![" Checks the connection.".into()];
        let out = render(
            &q,
            &TableMap::new(),
            &PostgresDriver::new(&Options::default()),
        )
        .unwrap();
        assert!(
            out.contains("/// Checks the connection.\npub fn ping(conn) {"),
            "{out}"
        );
    }
}
