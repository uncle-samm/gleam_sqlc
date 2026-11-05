use std::collections::{BTreeMap, HashMap};

use crate::driver::postgres::PostgresDriver;
use crate::driver::sqlite::SqliteDriver;
use crate::driver::traits::{Driver, path_alias};
use crate::driver::type_map::{EnumMap, TypeContext};
use crate::generators::models::{ModelsPlan, generate_models};
use crate::generators::naming::to_gleam_name;
use crate::generators::queries::generate_queries;
use crate::options::Options;
use crate::plugin::plugin::{Catalog, File, GenerateRequest, GenerateResponse, Query, Table};

/// A lookup of table name -> Table for resolving sqlc.embed() references.
/// Keys include both the plain name and the schema-qualified name.
pub type TableMap = HashMap<String, Table>;

/// Generate Gleam code from a sqlc GenerateRequest.
///
/// Any problem (bad options, unsupported engine or command, name clashes) is
/// returned as an error so sqlc fails loudly instead of writing broken code.
pub fn generate(request: GenerateRequest) -> Result<GenerateResponse, String> {
    let options = Options::parse(&request.plugin_options)?;
    let settings = request.settings.as_ref();
    let engine = settings.map(|s| s.engine.as_str()).unwrap_or("");

    let driver: Box<dyn Driver> = match engine {
        "postgresql" => Box::new(PostgresDriver::new(&options)),
        "sqlite" => {
            if options.uuid_as_string {
                return Err("uuidAsString is only supported for the postgresql engine".into());
            }
            Box::new(SqliteDriver::new(&options))
        }
        other => {
            return Err(format!(
                "engine '{other}' is not supported by sqlc-gen-gleam (use postgresql or sqlite)"
            ));
        }
    };

    let table_map = build_table_map(request.catalog.as_ref());
    let empty_catalog = Catalog::default();
    let catalog = request.catalog.as_ref().unwrap_or(&empty_catalog);
    let plan = ModelsPlan::new(catalog);
    let enums: EnumMap = plan.enum_map();

    let mut files = Vec::new();
    let write_models = !options.skip_models && (!plan.tables.is_empty() || !plan.enums.is_empty());
    if write_models {
        files.push(File {
            name: "models.gleam".into(),
            contents: generate_models(&plan, &enums, driver.as_ref()).into_bytes(),
        });
    }

    // Enum types are only usable from query modules when models.gleam exists
    // and we know its Gleam module path.
    let out_dir = settings
        .and_then(|s| s.codegen.as_ref())
        .map(|c| c.out.as_str())
        .unwrap_or("");
    let models_import = if write_models && !enums.is_empty() {
        let path = options
            .models_module
            .clone()
            .or_else(|| models_module_from_out(out_dir));
        if path.is_none() {
            eprintln!(
                "warning: cannot work out the Gleam module path of models.gleam from out '{out_dir}' \
                 (it is not under a src/ directory); enum columns are typed as String. \
                 Set the modelsModule option to fix this."
            );
        }
        path
    } else {
        None
    };
    let ctx = TypeContext {
        enums: &enums,
        enum_qualifier: models_import
            .as_ref()
            .map(|p| format!("{}.", path_alias(p))),
    };

    for (filename, queries) in &group_queries_by_file(&request.queries) {
        let module_name = filename_to_module(filename);
        if write_models && module_name == "models" {
            return Err(format!(
                "queries file '{filename}' would generate models.gleam, which clashes with the generated models; rename it or set skipModels"
            ));
        }
        let code = generate_queries(
            queries,
            &table_map,
            &options.overrides,
            &ctx,
            models_import.as_deref(),
            driver.as_ref(),
        )
        .map_err(|e| format!("{filename}: {e}"))?;
        files.push(File {
            name: format!("{module_name}.gleam"),
            contents: code.into_bytes(),
        });
    }

    Ok(GenerateResponse { files })
}

/// Derive the Gleam module path of `<out>/models.gleam`: the part of `out`
/// after its last `src/` component. `None` if `out` is not inside `src/`.
fn models_module_from_out(out: &str) -> Option<String> {
    let parts: Vec<&str> = out
        .split('/')
        .filter(|p| !p.is_empty() && *p != ".")
        .collect();
    let src_pos = parts.iter().rposition(|p| *p == "src")?;
    let mut module: Vec<&str> = parts[src_pos + 1..].to_vec();
    module.push("models");
    Some(module.join("/"))
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
fn group_queries_by_file(queries: &[Query]) -> BTreeMap<String, Vec<&Query>> {
    let mut groups: BTreeMap<String, Vec<&Query>> = BTreeMap::new();
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
    let base = filename.rsplit('/').next().unwrap_or(filename);
    to_gleam_name(base.strip_suffix(".sql").unwrap_or(base))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filename_to_module() {
        assert_eq!(filename_to_module("query.sql"), "query");
        assert_eq!(filename_to_module("user_queries.sql"), "user_queries");
        assert_eq!(filename_to_module("path/to/query.sql"), "query");
        assert_eq!(filename_to_module("user-queries.sql"), "user_queries");
    }

    #[test]
    fn test_models_module_from_out() {
        assert_eq!(
            models_module_from_out("src/generated").as_deref(),
            Some("generated/models")
        );
        assert_eq!(
            models_module_from_out("./src/db/sql/").as_deref(),
            Some("db/sql/models")
        );
        assert_eq!(
            models_module_from_out("app/src/generated").as_deref(),
            Some("generated/models")
        );
        assert_eq!(models_module_from_out("src").as_deref(), Some("models"));
        assert_eq!(models_module_from_out("generated"), None);
    }

    #[test]
    fn test_group_queries_empty() {
        let groups = group_queries_by_file(&[]);
        assert!(groups.is_empty());
    }

    #[test]
    fn unsupported_engine_is_an_error() {
        let request = GenerateRequest {
            settings: Some(crate::plugin::plugin::Settings {
                engine: "mysql".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = generate(request).unwrap_err();
        assert!(err.contains("mysql"), "{err}");
    }
}
