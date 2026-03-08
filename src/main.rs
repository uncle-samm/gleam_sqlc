mod codegen;
mod driver;
mod generators;
mod options;
mod plugin;
mod utils;

use prost::Message;
use std::io::{self, Read, Write};

use plugin::plugin::GenerateRequest;

fn main() {
    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input).unwrap_or_else(|e| {
        eprintln!("failed to read stdin: {e}");
        std::process::exit(1);
    });

    let request = GenerateRequest::decode(input.as_slice()).unwrap_or_else(|e| {
        eprintln!("failed to decode GenerateRequest: {e}");
        std::process::exit(1);
    });

    let response = codegen::generate(request);

    let output = response.encode_to_vec();
    io::stdout().write_all(&output).unwrap_or_else(|e| {
        eprintln!("failed to write stdout: {e}");
        std::process::exit(1);
    });
}
