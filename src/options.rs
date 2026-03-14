use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Options {
    #[serde(default = "default_postgleam_version")]
    pub postgleam_version: String,

    #[serde(default)]
    pub overrides: Vec<TypeOverride>,

    /// Custom module for generated imports and function calls.
    /// Replaces the driver default ("postgleam" for PostgreSQL, "glite" for SQLite).
    /// Example: "db/pg" generates `import db/pg` and calls like `pg.query_one(...)`.
    #[serde(default)]
    pub module: Option<String>,

    /// Custom decode module for generated decoder imports.
    /// Defaults to "{module}/decode". The last path segment is used as the alias
    /// in generated code, so it should end with "/decode" for compatibility.
    /// Example: "db/pg/decode" generates `import db/pg/decode`.
    #[serde(default)]
    pub decode_module: Option<String>,

    /// Skip generating models.gleam (enums + table types).
    /// Useful when you define your own domain types and only want query functions.
    #[serde(default)]
    pub skip_models: bool,

    /// Map PostgreSQL `uuid` columns to `String` (with `text` param/decoder)
    /// instead of `BitArray` (with `uuid` param/decoder).
    /// Useful when your app passes UUIDs as text strings.
    #[serde(default)]
    pub uuid_as_string: bool,

    /// Query timeout in milliseconds for PostgreSQL pool queries.
    /// Default: 5000 (5 seconds). Set to 0 to omit the timeout parameter.
    /// postgleam's pool.query_with/pool.query require a timeout argument.
    #[serde(default = "default_query_timeout")]
    pub query_timeout: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeOverride {
    pub column: String,
    pub gleam_type: Option<GleamTypeOverride>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GleamTypeOverride {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub not_null: bool,
}

fn default_postgleam_version() -> String {
    "0.6.0".to_string()
}

fn default_query_timeout() -> i64 {
    5000
}

impl Options {
    pub fn parse(data: &[u8]) -> Self {
        if data.is_empty() {
            return Self::default();
        }
        serde_json::from_slice(data).unwrap_or_else(|e| {
            eprintln!("warning: failed to parse plugin options: {e}");
            Self::default()
        })
    }
}
