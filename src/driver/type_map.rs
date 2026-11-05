//! Shared types for column type resolution across all drivers.

use std::collections::HashMap;

/// Gleam type info for a database column type.
#[derive(Debug, Clone)]
pub struct GleamType {
    /// The Gleam type expression, e.g. "Int", "String", "models.Status".
    pub type_name: String,
    /// Gleam function (or function expression) building one driver value,
    /// e.g. "value.Integer", "glite.int", "encode_status".
    pub param_fn: String,
    /// Decoder expression, e.g. "decode.int".
    pub decoder_fn: String,
    /// Arity of the param constructor (1 = normal, 2+ = tuple destructure needed).
    pub param_arity: u8,
    /// UUID kept as a String in Gleam: must be parsed before it can be sent.
    pub uuid_string: bool,
}

impl GleamType {
    pub fn simple(type_name: &str, param_fn: &str, decoder_fn: &str) -> Self {
        Self::multi(type_name, param_fn, decoder_fn, 1)
    }

    pub fn multi(type_name: &str, param_fn: &str, decoder_fn: &str, arity: u8) -> Self {
        assert!(
            (1..=4).contains(&arity),
            "param arity must be 1..=4, got {arity}"
        );
        Self {
            type_name: type_name.into(),
            param_fn: param_fn.into(),
            decoder_fn: decoder_fn.into(),
            param_arity: arity,
            uuid_string: false,
        }
    }
}

/// A fully resolved column or parameter type.
#[derive(Debug, Clone)]
pub struct ResolvedType {
    pub type_expr: String,
    pub param_expr: ParamExpr,
    pub decoder_expr: DecoderExpr,
}

impl ResolvedType {
    /// Build a resolved type from a base type, wrapping it for nullability / arrays.
    pub fn from_base(
        base: GleamType,
        nullable: bool,
        array: bool,
        array_decoder: Option<String>,
    ) -> Self {
        let elem_type = base.type_name.clone();
        let type_inner = if array {
            format!("List({elem_type})")
        } else {
            elem_type
        };
        let type_expr = if nullable {
            format!("Option({type_inner})")
        } else {
            type_inner
        };

        let param_expr = if base.uuid_string {
            ParamExpr::UuidString { nullable, array }
        } else if array {
            ParamExpr::Array {
                elem_fn: base.param_fn.clone(),
                arity: base.param_arity,
                nullable,
            }
        } else {
            ParamExpr::Value {
                fn_name: base.param_fn.clone(),
                arity: base.param_arity,
                nullable,
            }
        };

        let decoder_inner = match array_decoder {
            Some(d) if array => d,
            _ => base.decoder_fn,
        };
        let decoder_expr = if nullable {
            DecoderExpr::Optional {
                inner: decoder_inner,
            }
        } else {
            DecoderExpr::Direct {
                expr: decoder_inner,
            }
        };

        Self {
            type_expr,
            param_expr,
            decoder_expr,
        }
    }
}

/// How a Gleam value becomes a query parameter. Rendering is driver specific
/// (see `Driver::render_param`).
#[derive(Debug, Clone, PartialEq)]
pub enum ParamExpr {
    /// A single value built with `fn_name`. Arity > 1 means the Gleam value is a
    /// tuple that is destructured into the constructor's arguments.
    Value {
        fn_name: String,
        arity: u8,
        nullable: bool,
    },
    /// A list of values encoded as a PostgreSQL array.
    Array {
        elem_fn: String,
        arity: u8,
        nullable: bool,
    },
    /// A UUID held as a String: parsed before the query runs, failing with
    /// an EncodeError instead of silently sending NULL or bad bytes.
    UuidString { nullable: bool, array: bool },
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecoderExpr {
    Direct { expr: String },
    Optional { inner: String },
}

impl DecoderExpr {
    /// Generate the Gleam expression for a column decoder.
    pub fn to_gleam(&self) -> String {
        match self {
            DecoderExpr::Direct { expr } => expr.clone(),
            DecoderExpr::Optional { inner } => format!("decode.optional({inner})"),
        }
    }
}

/// Destructuring pattern and argument list for a tuple of the given arity.
pub fn tuple_pattern(arity: u8) -> (&'static str, &'static str) {
    match arity {
        2 => ("#(a, b)", "a, b"),
        3 => ("#(a, b, c)", "a, b, c"),
        4 => ("#(a, b, c, d)", "a, b, c, d"),
        other => unreachable!("tuple_pattern called with arity {other}"),
    }
}

/// A Gleam expression of type `fn(a) -> Value` for a constructor of the given arity.
pub fn value_fn(fn_name: &str, arity: u8) -> String {
    if arity <= 1 {
        fn_name.to_string()
    } else {
        let (pattern, args) = tuple_pattern(arity);
        format!("fn(t) {{ case t {{ {pattern} -> {fn_name}({args}) }} }}")
    }
}

/// A PostgreSQL enum known from the catalog.
#[derive(Debug, Clone)]
pub struct EnumInfo {
    /// Gleam type name, e.g. "Status".
    pub type_name: String,
    /// snake_case base used for helpers, e.g. "status".
    pub snake: String,
}

/// Lookup from catalog enum name ("status" or "schema.status") to its Gleam info.
pub type EnumMap = HashMap<String, EnumInfo>;

/// Context for resolving column types in a particular output module.
pub struct TypeContext<'a> {
    pub enums: &'a EnumMap,
    /// How enum types are referenced from the module being generated:
    /// `Some("")` inside models.gleam, `Some("models.")` in a query module,
    /// `None` when enum types are unavailable (enums become String).
    pub enum_qualifier: Option<String>,
}

impl<'a> TypeContext<'a> {
    #[cfg(test)]
    pub fn without_enums(enums: &'a EnumMap) -> Self {
        Self {
            enums,
            enum_qualifier: None,
        }
    }
}
