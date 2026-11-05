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
    if let Err(e) = io::stdin().read_to_end(&mut input) {
        fail(&format!("failed to read stdin: {e}"));
    }

    let request = match GenerateRequest::decode(input.as_slice()) {
        Ok(r) => r,
        Err(e) => fail(&format!("failed to decode GenerateRequest: {e}")),
    };

    let response = match codegen::generate(request) {
        Ok(r) => r,
        Err(e) => fail(&e),
    };

    let output = response.encode_to_vec();
    if let Err(e) = io::stdout().write_all(&output) {
        fail(&format!("failed to write stdout: {e}"));
    }
}

fn fail(message: &str) -> ! {
    eprintln!("sqlc-gen-gleam: {message}");
    std::process::exit(1);
}
