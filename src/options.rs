use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Options {
    #[serde(default = "default_postgleam_version")]
    pub postgleam_version: String,

    #[serde(default)]
    pub overrides: Vec<TypeOverride>,
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
    "0.3.0".to_string()
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
