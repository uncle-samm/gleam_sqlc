use std::collections::BTreeMap;

use serde::Deserialize;

/// Plugin options from the `options:` block of a sqlc codegen entry.
///
/// Unknown keys are rejected so a typo (e.g. `uuidAsStrign`) fails generation
/// instead of being silently ignored.
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Options {
    /// Deprecated and unused. Accepted so existing configs keep working.
    #[serde(default)]
    pub postgleam_version: Option<String>,

    #[serde(default)]
    pub overrides: Vec<TypeOverride>,

    /// Database types sqlc does not describe to plugins, such as PostgreSQL
    /// domains, mapped to a type the plugin knows: `{"posint": "integer"}`.
    /// Without an alias such a type is a String.
    #[serde(default)]
    pub type_aliases: BTreeMap<String, String>,

    /// Type array elements as `Option`, so arrays holding NULLs can be read
    /// and written: `List(Option(Int))` instead of `List(Int)`.
    #[serde(default)]
    pub nullable_array_elements: bool,

    /// Custom module for generated imports and function calls.
    /// Replaces the driver default ("postgleam" for PostgreSQL, "glite" for SQLite).
    /// Example: "db/pg" generates `import db/pg` and calls like `pg.query_with(...)`.
    /// "postgleam/pool" targets postgleam's connection pool.
    #[serde(default)]
    pub module: Option<String>,

    /// Custom decode module for generated decoder imports.
    /// Defaults to "{module}/decode" (or "postgleam/decode" when module is
    /// "postgleam/pool"). Its last path segment must be `decode`.
    #[serde(default)]
    pub decode_module: Option<String>,

    /// Gleam module path of the generated models file, used to import enum
    /// types into query modules. Defaults to the `out` directory relative to
    /// `src/` plus `/models` (e.g. out "src/db" -> "db/models").
    #[serde(default)]
    pub models_module: Option<String>,

    /// Skip generating models.gleam (enums + table types).
    /// Enum columns are then typed as String.
    #[serde(default)]
    pub skip_models: bool,

    /// Map PostgreSQL `uuid` columns to `String` instead of `BitArray`.
    /// Params are parsed with `value.uuid_from_string`; an invalid UUID makes
    /// the query function return `EncodeError` without touching the database.
    #[serde(default)]
    pub uuid_as_string: bool,

    /// Deprecated and unused. postgleam 0.9 pools take their timeouts from
    /// the pool config. Accepted so existing configs keep working.
    #[serde(default)]
    pub query_timeout: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TypeOverride {
    /// `QueryName:column` or `*:column`.
    pub column: String,
    /// The Gleam type to use. Exactly one of `gleamType` and `dbType` is set.
    #[serde(default)]
    pub gleam_type: Option<GleamTypeOverride>,
    /// The database type the column really has, for when sqlc infers the
    /// wrong one (e.g. `sum()` of a numeric column, `max()` of a timestamp).
    #[serde(default)]
    pub db_type: Option<DbTypeOverride>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DbTypeOverride {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub not_null: bool,
    #[serde(default)]
    pub array: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GleamTypeOverride {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub not_null: bool,
}

impl Options {
    /// Parse and validate plugin options. Any problem is an error: generation
    /// must not continue with settings the user did not ask for.
    pub fn parse(data: &[u8]) -> Result<Self, String> {
        let options: Options = if data.is_empty() {
            Self::default()
        } else {
            serde_json::from_slice(data).map_err(|e| format!("invalid plugin options: {e}"))?
        };
        options.validate()?;
        if options.postgleam_version.is_some() {
            eprintln!("warning: the postgleamVersion option is deprecated and has no effect");
        }
        if options.query_timeout.is_some() {
            eprintln!(
                "warning: the queryTimeout option is deprecated and has no effect; set timeouts on the postgleam pool config instead"
            );
        }
        Ok(options)
    }

    fn validate(&self) -> Result<(), String> {
        for path in [&self.module, &self.decode_module, &self.models_module]
            .into_iter()
            .flatten()
        {
            if !is_valid_module_path(path) {
                return Err(format!("'{path}' is not a valid Gleam module path"));
            }
        }
        if let Some(d) = &self.decode_module
            && d.rsplit('/').next() != Some("decode")
        {
            return Err(format!(
                "decodeModule '{d}' must end in '/decode' (generated code refers to it as `decode`)"
            ));
        }
        for o in &self.overrides {
            let parts: Vec<&str> = o.column.splitn(2, ':').collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                return Err(format!(
                    "override column '{}' must look like 'QueryName:column' or '*:column'",
                    o.column
                ));
            }
            match (&o.gleam_type, &o.db_type) {
                (Some(g), None) => {
                    if !matches!(
                        g.type_name.as_str(),
                        "String" | "Int" | "Float" | "Bool" | "BitArray"
                    ) {
                        return Err(format!(
                            "override type '{}' for '{}' is not supported (use String, Int, Float, Bool or BitArray, or name a database type with dbType)",
                            g.type_name, o.column
                        ));
                    }
                }
                (None, Some(d)) => {
                    if d.type_name.trim().is_empty() {
                        return Err(format!("override for '{}' has an empty dbType", o.column));
                    }
                }
                _ => {
                    return Err(format!(
                        "override for '{}' must set exactly one of gleamType and dbType",
                        o.column
                    ));
                }
            }
        }
        for (alias, target) in &self.type_aliases {
            if alias.trim().is_empty() || target.trim().is_empty() {
                return Err("typeAliases must not hold empty type names".into());
            }
        }
        Ok(())
    }
}

fn is_valid_module_path(path: &str) -> bool {
    !path.is_empty()
        && path.split('/').all(|seg| {
            !seg.is_empty()
                && seg.starts_with(|c: char| c.is_ascii_lowercase())
                && seg
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_options_are_default() {
        let o = Options::parse(b"").unwrap();
        assert!(o.module.is_none());
    }

    #[test]
    fn unknown_key_is_an_error() {
        let err = Options::parse(br#"{"uuidAsStrign": true}"#).unwrap_err();
        assert!(err.contains("uuidAsStrign"), "{err}");
    }

    #[test]
    fn wrong_type_is_an_error() {
        assert!(Options::parse(br#"{"skipModels": "yes"}"#).is_err());
    }

    #[test]
    fn deprecated_query_timeout_is_accepted() {
        let o = Options::parse(br#"{"module": "postgleam/pool", "queryTimeout": 5000}"#).unwrap();
        assert_eq!(o.query_timeout, Some(5000));
    }

    #[test]
    fn bad_paths_and_overrides_are_errors() {
        assert!(Options::parse(br#"{"module": "Db/Pg"}"#).is_err());
        assert!(Options::parse(br#"{"decodeModule": "db/decoders"}"#).is_err());
        assert!(
            Options::parse(
                br#"{"overrides": [{"column": "bio", "gleamType": {"type": "String"}}]}"#
            )
            .is_err()
        );
        assert!(
            Options::parse(
                br#"{"overrides": [{"column": "*:bio", "gleamType": {"type": "Decimal"}}]}"#
            )
            .is_err()
        );
        assert!(Options::parse(br#"{"overrides": [{"column": "*:bio", "gleamType": {"type": "String", "notNull": true}}]}"#).is_ok());
    }

    #[test]
    fn db_type_overrides_and_aliases() {
        let o = Options::parse(
            br#"{"typeAliases": {"posint": "integer"}, "nullableArrayElements": true,
                 "overrides": [{"column": "Stats:total", "dbType": {"type": "numeric", "array": true}}]}"#,
        )
        .unwrap();
        assert_eq!(o.type_aliases["posint"], "integer");
        assert!(o.nullable_array_elements);
        let d = o.overrides[0].db_type.as_ref().unwrap();
        assert_eq!(
            (d.type_name.as_str(), d.not_null, d.array),
            ("numeric", false, true)
        );

        // Exactly one of gleamType and dbType.
        let err = Options::parse(br#"{"overrides": [{"column": "*:bio"}]}"#).unwrap_err();
        assert!(err.contains("exactly one"), "{err}");
        assert!(Options::parse(
            br#"{"overrides": [{"column": "*:bio", "gleamType": {"type": "Int"}, "dbType": {"type": "int4"}}]}"#
        )
        .is_err());
        assert!(Options::parse(br#"{"typeAliases": {"posint": ""}}"#).is_err());
    }

    #[test]
    fn deprecated_postgleam_version_is_accepted() {
        assert!(Options::parse(br#"{"postgleamVersion": "0.6.0"}"#).is_ok());
    }
}
