use std::collections::{HashMap, HashSet};

use crate::driver::traits::{Driver, uses_name};
use crate::driver::type_map::{EnumInfo, EnumMap, TypeContext};
use crate::generators::naming::{
    column_to_field_name, doc_comment_lines, escape_gleam_string, table_to_type_name,
    to_gleam_upper_name, to_pascal_case, to_snake_case,
};
use crate::plugin::plugin::{Catalog, Table};
use crate::utils::CodeWriter;

/// A catalog enum with its final Gleam names.
#[derive(Debug, Clone)]
pub struct EnumPlan {
    /// Lookup keys used by column types: "name" for the default schema, else "schema.name".
    pub key: String,
    pub type_name: String,
    /// Base for the `{snake}_to_string` / `{snake}_from_string` helpers.
    pub snake: String,
    pub comment: String,
    /// (database value, Gleam constructor)
    pub variants: Vec<(String, String)>,
}

/// A catalog table with its final Gleam type name.
pub struct TablePlan<'a> {
    pub table: &'a Table,
    pub type_name: String,
}

/// Names for every table and enum in the catalog, chosen so that the generated
/// models module compiles: constructors are unique, type names are unique, and
/// every name is a valid Gleam identifier.
pub struct ModelsPlan<'a> {
    pub tables: Vec<TablePlan<'a>>,
    pub enums: Vec<EnumPlan>,
}

impl<'a> ModelsPlan<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        let default_schema = default_schema(catalog);
        let qualify = |schema: &str, name: &str| {
            if schema.is_empty() || schema == default_schema {
                name.to_string()
            } else {
                format!("{schema}_{name}")
            }
        };

        // Table types first: their names are also their record constructors.
        let mut type_names: HashMap<String, usize> = HashMap::new();
        let mut tables = Vec::new();
        for schema in &catalog.schemas {
            for table in &schema.tables {
                let name = table
                    .rel
                    .as_ref()
                    .map(|r| r.name.as_str())
                    .unwrap_or("unknown");
                let base = table_to_type_name(&qualify(&schema.name, name));
                let type_name = unique(&base, &mut type_names);
                tables.push(TablePlan { table, type_name });
            }
        }

        // Enum type names, avoiding clashes with table types.
        let mut enums = Vec::new();
        for schema in &catalog.schemas {
            for e in &schema.enums {
                let qualified = qualify(&schema.name, &e.name);
                let base = to_gleam_upper_name(&qualified)
                    .unwrap_or_else(|| format!("E{}", to_pascal_case(&qualified)));
                let base = if type_names.contains_key(&base) {
                    format!("{base}Enum")
                } else {
                    base
                };
                let type_name = unique(&base, &mut type_names);
                let key = if schema.name.is_empty() || schema.name == default_schema {
                    e.name.clone()
                } else {
                    format!("{}.{}", schema.name, e.name)
                };
                let variants = e
                    .vals
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let ctor = to_gleam_upper_name(v).unwrap_or_else(|| {
                            let cleaned: String =
                                v.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
                            if cleaned.is_empty() {
                                format!("{type_name}Value{i}")
                            } else {
                                format!("{type_name}{cleaned}")
                            }
                        });
                        (v.clone(), ctor)
                    })
                    .collect();
                enums.push(EnumPlan {
                    key,
                    snake: to_snake_case(&type_name),
                    type_name,
                    comment: e.comment.clone(),
                    variants,
                });
            }
        }

        // Constructors share one namespace per module. Any enum whose
        // constructors clash with another constructor gets all of its
        // constructors prefixed with the enum type name.
        let mut counts: HashMap<String, usize> = HashMap::new();
        for t in &tables {
            *counts.entry(t.type_name.clone()).or_default() += 1;
        }
        for e in &enums {
            for (_, c) in &e.variants {
                *counts.entry(c.clone()).or_default() += 1;
            }
        }
        for e in &mut enums {
            let clashes = e.variants.iter().any(|(_, c)| counts[c] > 1);
            if clashes {
                for (_, c) in &mut e.variants {
                    if !c.starts_with(&e.type_name) {
                        *c = format!("{}{c}", e.type_name);
                    }
                }
            }
        }
        // Last resort: still duplicated (e.g. "a-b" and "a_b") → number them.
        let mut taken: HashSet<String> = tables.iter().map(|t| t.type_name.clone()).collect();
        for e in &mut enums {
            for (_, c) in &mut e.variants {
                if !taken.insert(c.clone()) {
                    let mut n = 2;
                    while !taken.insert(format!("{c}{n}")) {
                        n += 1;
                    }
                    *c = format!("{c}{n}");
                }
            }
        }

        Self { tables, enums }
    }

    pub fn enum_map(&self) -> EnumMap {
        self.enums
            .iter()
            .map(|e| {
                (
                    e.key.clone(),
                    EnumInfo {
                        type_name: e.type_name.clone(),
                        snake: e.snake.clone(),
                    },
                )
            })
            .collect()
    }
}

fn default_schema(catalog: &Catalog) -> &str {
    if catalog.default_schema.is_empty() {
        "public"
    } else {
        &catalog.default_schema
    }
}

fn unique(base: &str, seen: &mut HashMap<String, usize>) -> String {
    let count = seen.entry(base.to_string()).or_insert(0);
    *count += 1;
    if *count == 1 {
        base.to_string()
    } else {
        let name = format!("{base}{count}");
        seen.insert(name.clone(), 1);
        name
    }
}

/// Generate the models.gleam file from the catalog.
pub fn generate_models(plan: &ModelsPlan, enums: &EnumMap, driver: &dyn Driver) -> String {
    let ctx = TypeContext {
        enums,
        enum_qualifier: Some(String::new()),
    };
    let mut body = CodeWriter::new();

    let mut first = true;
    for e in &plan.enums {
        if !first {
            body.blank();
        }
        first = false;
        generate_enum(e, &mut body);
    }
    for t in &plan.tables {
        if !first {
            body.blank();
        }
        first = false;
        generate_table_type(t, &ctx, &mut body, driver);
    }
    let body = body.into_string();

    let mut w = CodeWriter::new();
    w.line("// Code generated by sqlc-gen-gleam. DO NOT EDIT.");
    w.blank();
    if uses_name(&body, "Option") {
        w.line("import gleam/option.{type Option}");
        w.blank();
    }
    let mut out = w.into_string();
    out.push_str(&body);
    out
}

fn generate_table_type(
    plan: &TablePlan,
    ctx: &TypeContext,
    w: &mut CodeWriter,
    driver: &dyn Driver,
) {
    for line in doc_comment_lines(&plan.table.comment) {
        w.line(&line);
    }
    let type_name = &plan.type_name;
    w.writef(format_args!("pub type {type_name} {{"));
    w.indent();
    let mut seen: HashMap<String, usize> = HashMap::new();
    let fields: Vec<String> = plan
        .table
        .columns
        .iter()
        .map(|col| {
            let field_name = unique_field(&column_to_field_name(&col.name), &mut seen);
            let resolved = driver.resolve_column_type(col, ctx);
            format!("{field_name}: {}", resolved.type_expr)
        })
        .collect();
    w.writef(format_args!("{type_name}({})", fields.join(", ")));
    w.dedent();
    w.line("}");
}

fn unique_field(base: &str, seen: &mut HashMap<String, usize>) -> String {
    let count = seen.entry(base.to_string()).or_insert(0);
    *count += 1;
    if *count == 1 {
        base.to_string()
    } else {
        format!("{base}_{count}")
    }
}

fn generate_enum(e: &EnumPlan, w: &mut CodeWriter) {
    let type_name = &e.type_name;
    let snake = &e.snake;
    for line in doc_comment_lines(&e.comment) {
        w.line(&line);
    }
    w.writef(format_args!("pub type {type_name} {{"));
    w.indent();
    for (_, variant) in &e.variants {
        w.line(variant);
    }
    w.dedent();
    w.line("}");

    w.blank();
    w.writef(format_args!(
        "pub fn {snake}_to_string(value: {type_name}) -> String {{"
    ));
    w.indent();
    w.line("case value {");
    w.indent();
    for (val, variant) in &e.variants {
        w.writef(format_args!(
            "{variant} -> \"{}\"",
            escape_gleam_string(val)
        ));
    }
    w.dedent();
    w.line("}");
    w.dedent();
    w.line("}");

    w.blank();
    w.writef(format_args!(
        "pub fn {snake}_from_string(value: String) -> Result({type_name}, Nil) {{"
    ));
    w.indent();
    w.line("case value {");
    w.indent();
    for (val, variant) in &e.variants {
        w.writef(format_args!(
            "\"{}\" -> Ok({variant})",
            escape_gleam_string(val)
        ));
    }
    w.line("_ -> Error(Nil)");
    w.dedent();
    w.line("}");
    w.dedent();
    w.line("}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::postgres::PostgresDriver;
    use crate::options::Options;
    use crate::plugin::plugin::{Column, Enum, Identifier, Schema};

    fn catalog() -> Catalog {
        Catalog {
            default_schema: "public".into(),
            schemas: vec![Schema {
                name: "public".into(),
                comment: String::new(),
                tables: vec![Table {
                    rel: Some(Identifier {
                        catalog: String::new(),
                        schema: "public".into(),
                        name: "users".into(),
                    }),
                    columns: vec![Column {
                        name: "st".into(),
                        not_null: true,
                        r#type: Some(Identifier {
                            catalog: String::new(),
                            schema: String::new(),
                            name: "status".into(),
                        }),
                        ..Default::default()
                    }],
                    comment: "line one\npub fn injected() { 1 }".into(),
                }],
                enums: vec![
                    Enum {
                        name: "status".into(),
                        vals: vec![
                            "active".into(),
                            "in progress".into(),
                            "2fa".into(),
                            "say \"hi\"".into(),
                        ],
                        comment: String::new(),
                    },
                    Enum {
                        name: "other_status".into(),
                        vals: vec!["active".into(), "inactive".into()],
                        comment: String::new(),
                    },
                ],
                composite_types: vec![],
            }],
            ..Default::default()
        }
    }

    #[test]
    fn enums_are_valid_and_unique() {
        let cat = catalog();
        let plan = ModelsPlan::new(&cat);
        let status = &plan.enums[0];
        let ctors: Vec<&str> = status.variants.iter().map(|(_, c)| c.as_str()).collect();
        // "active" clashes with other_status, so both enums are prefixed.
        assert_eq!(
            ctors,
            vec![
                "StatusActive",
                "StatusInProgress",
                "Status2fa",
                "StatusSayHi"
            ]
        );
        let other: Vec<&str> = plan.enums[1]
            .variants
            .iter()
            .map(|(_, c)| c.as_str())
            .collect();
        assert_eq!(other, vec!["OtherStatusActive", "OtherStatusInactive"]);
    }

    #[test]
    fn models_escape_strings_and_comments_and_use_enum_types() {
        let cat = catalog();
        let plan = ModelsPlan::new(&cat);
        let enums = plan.enum_map();
        let driver = PostgresDriver::new(&Options::default());
        let out = generate_models(&plan, &enums, &driver);
        assert!(out.contains("StatusSayHi -> \"say \\\"hi\\\"\""), "{out}");
        assert!(out.contains("/// pub fn injected() { 1 }"), "{out}");
        assert!(
            !out.lines().any(|l| l.starts_with("pub fn injected")),
            "{out}"
        );
        assert!(out.contains("User(st: Status)"), "{out}");
    }
}
