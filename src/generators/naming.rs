/// Convert a string to PascalCase for Gleam type names.
/// Handles snake_case ("get_author" -> "GetAuthor"), already-PascalCase ("GetAuthor" -> "GetAuthor"),
/// and camelCase ("listAuthors" -> "ListAuthors").
pub fn to_pascal_case(s: &str) -> String {
    // Split on underscores and hyphens
    let words: Vec<&str> = s.split(['_', '-']).filter(|p| !p.is_empty()).collect();

    words
        .iter()
        .flat_map(|word| {
            // For each underscore-separated part, also split on camelCase boundaries
            split_camel_case(word)
        })
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    let mut result = first.to_uppercase().to_string();
                    result.extend(chars.map(|c| c.to_ascii_lowercase()));
                    result
                }
            }
        })
        .collect()
}

/// Split a word on camelCase boundaries.
/// e.g. "GetAuthor" -> ["Get", "Author"], "listAuthors" -> ["list", "Authors"]
/// Single lowercase words are returned as-is: "author" -> ["author"]
fn split_camel_case(s: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();

    for (i, c) in s.chars().enumerate() {
        if i > 0 && c.is_uppercase() && !current.is_empty() {
            words.push(current);
            current = String::new();
        }
        current.push(c);
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// Convert a string to snake_case for Gleam function/field names.
/// e.g. "GetAuthor" -> "get_author", "listAuthors" -> "list_authors"
pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    let mut prev_was_upper = false;

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 && !prev_was_upper {
                result.push('_');
            } else if prev_was_upper {
                // Check if the next char is lowercase — if so, insert underscore before this char
                // e.g. "HTMLParser" -> "html_parser"
                let next_is_lower = s.chars().nth(i + 1).is_some_and(|n| n.is_lowercase());
                if i > 0 && next_is_lower {
                    result.push('_');
                }
            }
            result.push(c.to_ascii_lowercase());
            prev_was_upper = true;
        } else {
            result.push(c);
            prev_was_upper = false;
        }
    }

    result
}

/// Singularize a table name for use as a type name.
/// Simple heuristic with a list of words that end in "s" but are already singular
/// (or have no distinct singular form).
/// e.g. "authors" -> "author", "categories" -> "category", "news" -> "news"
pub fn singularize(s: &str) -> String {
    const UNCHANGED: &[&str] = &[
        "news",
        "series",
        "species",
        "data",
        "media",
        "status",
        "analysis",
        "metadata",
        "information",
        "equipment",
        "sms",
    ];
    let lower = s.to_ascii_lowercase();
    let last_word = lower.rsplit(['_', '-']).next().unwrap_or(&lower);
    if UNCHANGED.contains(&last_word)
        || last_word.ends_with("ss")
        || last_word.ends_with("us")
        || last_word.ends_with("is")
    {
        return s.to_string();
    }
    if let Some(stem) = s.strip_suffix("ies") {
        format!("{stem}y")
    } else if s.ends_with("sses")
        || s.ends_with("xes")
        || s.ends_with("zes")
        || s.ends_with("ches")
        || s.ends_with("shes")
    {
        s[..s.len() - 2].to_string()
    } else if let Some(stem) = s.strip_suffix("ses") {
        // "statuses" -> "status", "buses" -> "bus"
        format!("{stem}s")
    } else if let Some(stem) = s.strip_suffix('s') {
        stem.to_string()
    } else {
        s.to_string()
    }
}

/// Convert a table name to a Gleam type name.
/// e.g. "authors" -> "Author", "postgres_numeric_types" -> "PostgresNumericType"
pub fn table_to_type_name(table_name: &str) -> String {
    let singular = singularize(table_name);
    to_gleam_upper_name(&singular).unwrap_or_else(|| format!("T{}", to_pascal_case(&singular)))
}

/// Convert a query name to a Gleam function name.
/// e.g. "GetAuthor" -> "get_author", "ListAuthors" -> "list_authors"
pub fn query_to_fn_name(query_name: &str) -> String {
    to_gleam_name(query_name)
}

/// Convert a column name to a Gleam field name.
/// Already snake_case in SQL typically, but normalize just in case.
pub fn column_to_field_name(col_name: &str) -> String {
    to_gleam_name(col_name)
}

/// Gleam keywords (including reserved-for-future-use words) that cannot be used as names.
const RESERVED: &[&str] = &[
    "as",
    "assert",
    "auto",
    "case",
    "const",
    "delegate",
    "derive",
    "echo",
    "else",
    "fn",
    "if",
    "implement",
    "import",
    "let",
    "macro",
    "opaque",
    "panic",
    "pub",
    "test",
    "todo",
    "type",
    "use",
];

/// Check if a name is a Gleam reserved word and escape it if needed.
pub fn escape_reserved(name: &str) -> String {
    if RESERVED.contains(&name) {
        format!("{name}_")
    } else {
        name.to_string()
    }
}

/// Turn any SQL name into a valid Gleam lower-case name (variable, field or function).
/// Non-alphanumeric characters become `_`, runs of `_` collapse, a leading digit or
/// underscore gets an `x` prefix, and reserved words get a `_` suffix.
pub fn to_gleam_name(raw: &str) -> String {
    let snake = to_snake_case(raw);
    let mut out = String::with_capacity(snake.len());
    for c in snake.chars() {
        let c = if c.is_ascii_alphanumeric() {
            c.to_ascii_lowercase()
        } else {
            '_'
        };
        if c == '_' && (out.is_empty() || out.ends_with('_')) {
            continue;
        }
        out.push(c);
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        return "x".to_string();
    }
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert_str(0, "x_");
    }
    escape_reserved(&out)
}

/// Turn any SQL name into a valid Gleam upper-case name (type or constructor),
/// or `None` when it starts with a digit (the caller adds a prefix).
pub fn to_gleam_upper_name(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    let name = to_pascal_case(&cleaned);
    if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
        None
    } else {
        Some(name)
    }
}

/// Escape a string for use inside a Gleam string literal.
pub fn escape_gleam_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Render a free-form comment (possibly multi-line) as Gleam `///` doc lines.
pub fn doc_comment_lines(comment: &str) -> Vec<String> {
    comment
        .lines()
        .map(|l| {
            let l = l.trim_end();
            if l.is_empty() {
                "///".to_string()
            } else {
                format!("/// {}", l.trim_start_matches(' '))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("authors"), "Authors");
        assert_eq!(to_pascal_case("get_author"), "GetAuthor");
        assert_eq!(to_pascal_case("my_table_name"), "MyTableName");
        assert_eq!(to_pascal_case("id"), "Id");
        assert_eq!(
            to_pascal_case("postgres_numeric_types"),
            "PostgresNumericTypes"
        );
        // Hyphenated
        assert_eq!(to_pascal_case("claude-code"), "ClaudeCode");
        assert_eq!(to_pascal_case("api-key"), "ApiKey");
        assert_eq!(to_pascal_case("session-token"), "SessionToken");
        // Already PascalCase
        assert_eq!(to_pascal_case("GetAuthor"), "GetAuthor");
        assert_eq!(to_pascal_case("ListAuthors"), "ListAuthors");
        assert_eq!(
            to_pascal_case("CreateAuthorReturnId"),
            "CreateAuthorReturnId"
        );
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("GetAuthor"), "get_author");
        assert_eq!(to_snake_case("ListAuthors"), "list_authors");
        assert_eq!(
            to_snake_case("CreateAuthorReturnId"),
            "create_author_return_id"
        );
        assert_eq!(to_snake_case("already_snake"), "already_snake");
        assert_eq!(to_snake_case("HTMLParser"), "html_parser");
    }

    #[test]
    fn test_singularize() {
        assert_eq!(singularize("authors"), "author");
        assert_eq!(singularize("books"), "book");
        assert_eq!(singularize("categories"), "category");
        assert_eq!(singularize("status"), "status");
        assert_eq!(singularize("address"), "address");
        assert_eq!(singularize("boxes"), "box");
        assert_eq!(singularize("news"), "news");
        assert_eq!(singularize("series"), "series");
        assert_eq!(singularize("statuses"), "status");
        assert_eq!(singularize("user_settings"), "user_setting");
        assert_eq!(singularize("addresses"), "address");
        assert_eq!(singularize("matches"), "match");
    }

    #[test]
    fn test_to_gleam_name() {
        assert_eq!(to_gleam_name("first name"), "first_name");
        assert_eq!(to_gleam_name("2fa_code"), "x_2fa_code");
        assert_eq!(to_gleam_name("UserID"), "user_id");
        assert_eq!(to_gleam_name("type"), "type_");
        assert_eq!(to_gleam_name("derive"), "derive_");
        assert_eq!(to_gleam_name("__weird--name__"), "weird_name");
        assert_eq!(to_gleam_name(""), "x");
    }

    #[test]
    fn test_to_gleam_upper_name() {
        assert_eq!(
            to_gleam_upper_name("in progress").as_deref(),
            Some("InProgress")
        );
        assert_eq!(to_gleam_upper_name("say \"hi\"").as_deref(), Some("SayHi"));
        assert_eq!(to_gleam_upper_name("2fa"), None);
        assert_eq!(to_gleam_upper_name("!!!"), None);
    }

    #[test]
    fn test_escape_gleam_string() {
        assert_eq!(escape_gleam_string("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
        assert_eq!(escape_gleam_string("bell\u{7}"), "bell\\u{7}");
    }

    #[test]
    fn test_doc_comment_lines() {
        assert_eq!(
            doc_comment_lines("one\npub fn x() { 1 }"),
            vec!["/// one", "/// pub fn x() { 1 }"]
        );
    }

    #[test]
    fn test_table_to_type_name() {
        assert_eq!(table_to_type_name("authors"), "Author");
        assert_eq!(
            table_to_type_name("postgres_numeric_types"),
            "PostgresNumericType"
        );
    }

    #[test]
    fn test_query_to_fn_name() {
        assert_eq!(query_to_fn_name("GetAuthor"), "get_author");
        assert_eq!(query_to_fn_name("ListAuthors"), "list_authors");
    }

    #[test]
    fn test_escape_reserved() {
        assert_eq!(escape_reserved("type"), "type_");
        assert_eq!(escape_reserved("name"), "name");
        assert_eq!(escape_reserved("let"), "let_");
    }
}
