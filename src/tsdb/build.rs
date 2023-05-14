use std::path::Path;
use std::{env, fs};

use protobuf_codegen::Customize;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let generated_with_pure_dir = format!("{}/generated_with_pure", out_dir);

    if Path::new(&generated_with_pure_dir).exists() {
        fs::remove_dir_all(&generated_with_pure_dir).unwrap();
    }
    fs::create_dir(&generated_with_pure_dir).unwrap();

    protobuf_codegen::Codegen::new()
        .customize(Customize::default().gen_mod_rs(true))
        .out_dir(generated_with_pure_dir)
        // .out_dir("src/proto")
        .includes(&["src/meta"])
        .inputs(&["src/meta/meta.proto"])
        .run()
        .expect("protoc");
}
