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

    /// Query timeout in milliseconds, passed as the last argument of every call.
    /// Needed for APIs that take a timeout, such as `postgleam/pool`.
    /// Default: 5000 when module is "postgleam/pool", otherwise no timeout
    /// argument. Set to 0 to never pass one.
    #[serde(default)]
    pub query_timeout: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TypeOverride {
    /// `QueryName:column` or `*:column`.
    pub column: String,
    pub gleam_type: GleamTypeOverride,
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
        Ok(options)
    }

    fn validate(&self) -> Result<(), String> {
        if let Some(t) = self.query_timeout
            && t < 0
        {
            return Err(format!("queryTimeout must be >= 0, got {t}"));
        }
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
            if !matches!(
                o.gleam_type.type_name.as_str(),
                "String" | "Int" | "Float" | "Bool" | "BitArray"
            ) {
                return Err(format!(
                    "override type '{}' for '{}' is not supported (use String, Int, Float, Bool or BitArray)",
                    o.gleam_type.type_name, o.column
                ));
            }
        }
        Ok(())
    }

    /// Effective query timeout for the given module import path.
    pub fn effective_timeout(&self, import_path: &str) -> Option<i64> {
        match self.query_timeout {
            Some(0) => None,
            Some(t) => Some(t),
            None if import_path == "postgleam/pool" => Some(5000),
            None => None,
        }
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
        assert_eq!(o.effective_timeout("postgleam"), None);
        assert_eq!(o.effective_timeout("postgleam/pool"), Some(5000));
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
    fn timeout_zero_means_none() {
        let o = Options::parse(br#"{"module": "postgleam/pool", "queryTimeout": 0}"#).unwrap();
        assert_eq!(o.effective_timeout("postgleam/pool"), None);
        let o = Options::parse(br#"{"queryTimeout": 250}"#).unwrap();
        assert_eq!(o.effective_timeout("postgleam"), Some(250));
        assert!(Options::parse(br#"{"queryTimeout": -1}"#).is_err());
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
    fn deprecated_postgleam_version_is_accepted() {
        assert!(Options::parse(br#"{"postgleamVersion": "0.6.0"}"#).is_ok());
    }
}
