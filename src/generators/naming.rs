/// Convert a string to PascalCase for Gleam type names.
/// Handles snake_case ("get_author" -> "GetAuthor"), already-PascalCase ("GetAuthor" -> "GetAuthor"),
/// and camelCase ("listAuthors" -> "ListAuthors").
pub fn to_pascal_case(s: &str) -> String {
    // Split on underscores and hyphens
    let words: Vec<&str> = s.split(|c| c == '_' || c == '-').filter(|p| !p.is_empty()).collect();

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
        if i > 0 && c.is_uppercase() {
            if !current.is_empty() {
                words.push(current);
                current = String::new();
            }
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
/// Simple heuristic: strip trailing 's' if present.
/// e.g. "authors" -> "author", "books" -> "book", "status" -> "status"
pub fn singularize(s: &str) -> String {
    if s.ends_with("ies") {
        format!("{}y", &s[..s.len() - 3])
    } else if s.ends_with("ses") || s.ends_with("xes") || s.ends_with("zes") {
        s[..s.len() - 2].to_string()
    } else if s.ends_with('s') && !s.ends_with("ss") && !s.ends_with("us") {
        s[..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Convert a table name to a Gleam type name.
/// e.g. "authors" -> "Author", "postgres_numeric_types" -> "PostgresNumericType"
pub fn table_to_type_name(table_name: &str) -> String {
    to_pascal_case(&singularize(table_name))
}

/// Convert a query name to a Gleam function name.
/// e.g. "GetAuthor" -> "get_author", "ListAuthors" -> "list_authors"
pub fn query_to_fn_name(query_name: &str) -> String {
    to_snake_case(query_name)
}

/// Convert a column name to a Gleam field name.
/// Already snake_case in SQL typically, but normalize just in case.
pub fn column_to_field_name(col_name: &str) -> String {
    to_snake_case(col_name)
}

/// Check if a name is a Gleam reserved word and escape it if needed.
pub fn escape_reserved(name: &str) -> String {
    match name {
        "as" | "assert" | "auto" | "case" | "const" | "echo" | "else" | "fn" | "if"
        | "import" | "let" | "macro" | "opaque" | "panic" | "pub" | "test" | "todo"
        | "type" | "use" => format!("{name}_"),
        _ => name.to_string(),
    }
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
        assert_eq!(to_pascal_case("postgres_numeric_types"), "PostgresNumericTypes");
        // Hyphenated
        assert_eq!(to_pascal_case("claude-code"), "ClaudeCode");
        assert_eq!(to_pascal_case("api-key"), "ApiKey");
        assert_eq!(to_pascal_case("session-token"), "SessionToken");
        // Already PascalCase
        assert_eq!(to_pascal_case("GetAuthor"), "GetAuthor");
        assert_eq!(to_pascal_case("ListAuthors"), "ListAuthors");
        assert_eq!(to_pascal_case("CreateAuthorReturnId"), "CreateAuthorReturnId");
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("GetAuthor"), "get_author");
        assert_eq!(to_snake_case("ListAuthors"), "list_authors");
        assert_eq!(to_snake_case("CreateAuthorReturnId"), "create_author_return_id");
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
    }

    #[test]
    fn test_table_to_type_name() {
        assert_eq!(table_to_type_name("authors"), "Author");
        assert_eq!(table_to_type_name("postgres_numeric_types"), "PostgresNumericType");
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
