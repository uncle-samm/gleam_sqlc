fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/plugin/codegen.proto"], &["proto/"])?;
    Ok(())
}
