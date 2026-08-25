// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use std::path::Path;
use std::time::SystemTime;
use std::{env, fs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;

    // build protos
    let mut config = prost_build::Config::new();
    config
        .protoc_executable(protoc)
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".reduct.proto.auth", "#[serde(default)]")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .compile_protos(
            &[
                "src/proto/auth.proto",
                "src/proto/storage.proto",
                "src/proto/replication.proto",
            ],
            &["src/protos/"],
        )
        .expect("Failed to compile protos");

    #[cfg(feature = "web-console")]
    package_web_console();
    // get build time and commit
    let build_time = chrono::DateTime::<chrono::Utc>::from(SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let commit = match std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
    {
        Ok(output) => String::from_utf8(output.stdout).expect("Failed to get commit"),
        Err(_) => env::var("GIT_COMMIT").unwrap_or("unknown".to_string()),
    };

    println!("cargo:rustc-env=BUILD_TIME={}", build_time);
    println!("cargo:rustc-env=COMMIT={}", commit);
    Ok(())
}
#[cfg(feature = "web-console")]
fn package_web_console() {
    use std::path::PathBuf;

    println!("cargo:rerun-if-env-changed=WEB_CONSOLE_BUILD");
    let console_dir = match env::var("WEB_CONSOLE_BUILD") {
        Ok(path) => PathBuf::from(path),
        Err(_) => {
            Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap()).join("../../reductstore-web/build")
        }
    };

    if !console_dir.is_dir() {
        panic!(
            "Web console build not found at {:?}. Run `npm run build` in the reductstore-web checkout or set WEB_CONSOLE_BUILD to its build directory.",
            console_dir
        );
    }

    let out_dir = env::var("OUT_DIR").unwrap();
    let file =
        fs::File::create(format!("{}/console.zip", out_dir)).expect("Failed to create console.zip");
    let mut zip = zip::ZipWriter::new(file);
    add_dir_to_zip(&mut zip, &console_dir, &console_dir);
    zip.finish().expect("Failed to write console.zip");
}

#[cfg(feature = "web-console")]
fn add_dir_to_zip(zip: &mut zip::ZipWriter<fs::File>, root: &Path, dir: &Path) {
    use std::io::Write;

    for entry in fs::read_dir(dir).expect("Failed to read web console build directory") {
        let path = entry.expect("Failed to read directory entry").path();
        if path.is_dir() {
            let name = path
                .strip_prefix(root)
                .unwrap()
                .to_str()
                .expect("Non-UTF-8 path in web console build")
                .replace('\\', "/");
            zip.add_directory(name, zip::write::SimpleFileOptions::default())
                .expect("Failed to add directory to console.zip");
            add_dir_to_zip(zip, root, &path);
        } else {
            println!("cargo:rerun-if-changed={}", path.display());
            let name = path
                .strip_prefix(root)
                .unwrap()
                .to_str()
                .expect("Non-UTF-8 path in web console build")
                .replace('\\', "/");
            zip.start_file(name, zip::write::SimpleFileOptions::default())
                .expect("Failed to add file to console.zip");
            zip.write_all(&fs::read(&path).expect("Failed to read web console file"))
                .expect("Failed to write file to console.zip");
        }
    }
}
