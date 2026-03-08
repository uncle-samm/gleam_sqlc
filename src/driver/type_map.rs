/// Shared types for column type resolution across all drivers.

/// Gleam type info for a database column.
#[derive(Debug, Clone)]
pub struct GleamType {
    /// The Gleam type expression, e.g. "Int", "String", "Bool"
    pub type_name: String,
    /// Param constructor function, e.g. "postgleam.int", "glite.int"
    pub param_fn: String,
    /// Decoder function, e.g. "decode.int", "decode.text"
    pub decoder_fn: String,
    /// Arity of the param constructor (1 = normal, 2+ = tuple destructure needed)
    pub param_arity: u8,
}

impl GleamType {
    pub fn simple(type_name: &str, param_fn: &str, decoder_fn: &str) -> Self {
        Self {
            type_name: type_name.into(),
            param_fn: param_fn.into(),
            decoder_fn: decoder_fn.into(),
            param_arity: 1,
        }
    }

    pub fn multi(type_name: &str, param_fn: &str, decoder_fn: &str, arity: u8) -> Self {
        Self {
            type_name: type_name.into(),
            param_fn: param_fn.into(),
            decoder_fn: decoder_fn.into(),
            param_arity: arity,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedType {
    pub type_expr: String,
    pub param_expr: ParamExpr,
    pub decoder_expr: DecoderExpr,
}

#[derive(Debug, Clone)]
pub enum ParamExpr {
    /// Simple: fn_name(var)
    Direct { fn_name: String },
    /// Nullable: module.nullable(var, inner_fn)
    Nullable { inner_fn: String },
    /// Destructure 2-tuple: case var { #(a, b) -> fn_name(a, b) }
    Destruct2 { fn_name: String },
    /// Destructure 3-tuple: case var { #(a, b, c) -> fn_name(a, b, c) }
    Destruct3 { fn_name: String },
    /// Destructure 4-tuple: case var { #(a, b, c, d) -> fn_name(a, b, c, d) }
    Destruct4 { fn_name: String },
    /// Nullable with tuple destructuring
    NullableDestruct {
        fn_name: String,
        arity: u8,
    },
    /// Array encoding
    Array { inner_fn: String, nullable: bool },
}

#[derive(Debug, Clone)]
pub enum DecoderExpr {
    Direct { fn_name: String },
    Optional { inner_fn: String },
    Array { inner_fn: String, nullable: bool },
}

impl ParamExpr {
    /// Generate the Gleam expression to encode a parameter value.
    pub fn to_gleam(&self, var_name: &str, module_name: &str) -> String {
        match self {
            ParamExpr::Direct { fn_name } => format!("{fn_name}({var_name})"),
            ParamExpr::Nullable { inner_fn } => {
                format!("{module_name}.nullable({var_name}, {inner_fn})")
            }
            ParamExpr::Destruct2 { fn_name } => {
                format!("case {var_name} {{ #(a, b) -> {fn_name}(a, b) }}")
            }
            ParamExpr::Destruct3 { fn_name } => {
                format!("case {var_name} {{ #(a, b, c) -> {fn_name}(a, b, c) }}")
            }
            ParamExpr::Destruct4 { fn_name } => {
                format!("case {var_name} {{ #(a, b, c, d) -> {fn_name}(a, b, c, d) }}")
            }
            ParamExpr::NullableDestruct { fn_name, arity } => {
                let (pattern, args) = match arity {
                    2 => ("#(a, b)", "a, b"),
                    3 => ("#(a, b, c)", "a, b, c"),
                    4 => ("#(a, b, c, d)", "a, b, c, d"),
                    _ => ("#(a, b)", "a, b"),
                };
                format!(
                    "{module_name}.nullable({var_name}, fn(v) {{ case v {{ {pattern} -> {fn_name}({args}) }} }})"
                )
            }
            ParamExpr::Array { inner_fn, nullable } => {
                // postgleam.array takes List(Option(Value)).
                // Param constructors like postgleam.int already return Option(Value),
                // so we just map each element through the constructor.
                if *nullable {
                    format!(
                        "{module_name}.nullable({var_name}, fn(arr) {{ {module_name}.array(list.map(arr, {inner_fn})) }})"
                    )
                } else {
                    format!("{module_name}.array(list.map({var_name}, {inner_fn}))")
                }
            }
        }
    }
}

impl DecoderExpr {
    /// Generate the Gleam expression for a column decoder.
    pub fn to_gleam(&self) -> String {
        match self {
            DecoderExpr::Direct { fn_name } => fn_name.clone(),
            DecoderExpr::Optional { inner_fn } => format!("decode.optional({inner_fn})"),
            DecoderExpr::Array { inner_fn, nullable } => {
                if *nullable {
                    format!("decode.optional(decode.array({inner_fn}))")
                } else {
                    format!("decode.array({inner_fn})")
                }
            }
        }
    }
}
