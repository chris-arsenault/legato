//! Build script for compiling the versioned Legato protobuf schema.

fn main() {
    println!("cargo:rerun-if-changed=proto/legato/v1/legato.proto");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc is available");
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc);

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(config, &["proto/legato/v1/legato.proto"], &["proto"])
        .expect("proto compilation should succeed");
}
