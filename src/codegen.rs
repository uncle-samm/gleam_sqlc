use std::collections::{BTreeMap, HashMap};

use crate::driver::postgres::PostgresDriver;
use crate::driver::sqlite::SqliteDriver;
use crate::driver::traits::Driver;
use crate::generators::models::generate_models;
use crate::generators::queries::generate_queries;
use crate::options::Options;
use crate::plugin::plugin::{Catalog, File, GenerateRequest, GenerateResponse, Query, Table};

/// A lookup of table name -> Table for resolving sqlc.embed() references.
pub type TableMap = HashMap<String, Table>;

/// Generate Gleam code from a sqlc GenerateRequest.
pub fn generate(request: GenerateRequest) -> GenerateResponse {
    let options = Options::parse(&request.plugin_options);
    let mut files = Vec::new();

    // Select driver based on engine
    let engine = request
        .settings
        .as_ref()
        .map(|s| s.engine.as_str())
        .unwrap_or("postgresql");

    let driver: Box<dyn Driver> = match engine {
        "sqlite" => Box::new(SqliteDriver::new(&options)),
        "postgresql" | "" => Box::new(PostgresDriver::new(&options)),
        _ => {
            eprintln!("warning: unsupported engine '{engine}', defaulting to postgresql");
            Box::new(PostgresDriver::new(&options))
        }
    };

    // Build table map for embed resolution
    let table_map = build_table_map(request.catalog.as_ref());

    // Generate models from catalog
    if let Some(ref catalog) = request.catalog {
        let has_tables = catalog.schemas.iter().any(|s| !s.tables.is_empty());
        if has_tables {
            let models_code = generate_models(catalog, driver.as_ref());
            files.push(File {
                name: "models.gleam".into(),
                contents: models_code.into_bytes(),
            });
        }
    }

    // Group queries by source filename
    let query_groups = group_queries_by_file(&request.queries);

    for (filename, queries) in &query_groups {
        let module_name = filename_to_module(filename);
        let queries_code = generate_queries(queries, &module_name, &table_map, &options.overrides, driver.as_ref());
        files.push(File {
            name: format!("{module_name}.gleam"),
            contents: queries_code.into_bytes(),
        });
    }

    GenerateResponse { files }
}

/// Build a lookup map from table name to Table definition.
/// Keys include both plain name and schema-qualified name.
fn build_table_map(catalog: Option<&Catalog>) -> TableMap {
    let mut map = TableMap::new();
    if let Some(catalog) = catalog {
        for schema in &catalog.schemas {
            for table in &schema.tables {
                if let Some(ref rel) = table.rel {
                    map.insert(rel.name.clone(), table.clone());
                    if !rel.schema.is_empty() {
                        map.insert(format!("{}.{}", rel.schema, rel.name), table.clone());
                    }
                }
            }
        }
    }
    map
}

/// Group queries by their source SQL filename.
fn group_queries_by_file<'a>(queries: &'a [Query]) -> BTreeMap<String, Vec<&'a Query>> {
    let mut groups: BTreeMap<String, Vec<&'a Query>> = BTreeMap::new();
    for query in queries {
        let key = if query.filename.is_empty() {
            "query.sql".to_string()
        } else {
            query.filename.clone()
        };
        groups.entry(key).or_default().push(query);
    }
    groups
}

/// Convert a SQL filename to a Gleam module name.
fn filename_to_module(filename: &str) -> String {
    filename
        .rsplit('/')
        .next()
        .unwrap_or(filename)
        .trim_end_matches(".sql")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filename_to_module() {
        assert_eq!(filename_to_module("query.sql"), "query");
        assert_eq!(filename_to_module("user_queries.sql"), "user_queries");
        assert_eq!(filename_to_module("path/to/query.sql"), "query");
    }

    #[test]
    fn test_group_queries_empty() {
        let queries = vec![];
        let groups = group_queries_by_file(&queries);
        assert!(groups.is_empty());
    }
}
