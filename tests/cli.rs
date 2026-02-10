use std::path::Path;
use std::process::Command;

use cucumber::World;
use cucumber::then;
use cucumber::when;

const TEMPDIR_PLACEHOLDER: &str = "$TEMPDIR";

#[derive(Debug, Default, World)]
pub struct CliWorld {
    output: Option<std::process::Output>,
    temp_dir: Option<tempfile::TempDir>,
}

fn replace_tempdir(s: &str, temp_path: &str) -> String {
    s.replace(TEMPDIR_PLACEHOLDER, temp_path)
}

#[when(regex = r#"^I run `dtfu (.+)`$"#)]
fn run_dtfu_with_args(world: &mut CliWorld, args: String) {
    let args_str = args;
    let temp_path = if args_str.contains(TEMPDIR_PLACEHOLDER) {
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let path = temp_dir
            .path()
            .to_str()
            .expect("Temp path is not valid UTF-8")
            .to_string();
        world.temp_dir = Some(temp_dir);
        path
    } else {
        String::new()
    };

    let resolved_args = if temp_path.is_empty() {
        args_str
    } else {
        replace_tempdir(&args_str, &temp_path)
    };

    let args: Vec<&str> = resolved_args.split_whitespace().collect();
    let output = Command::new(env!("CARGO_BIN_EXE_dtfu"))
        .args(&args)
        .output()
        .expect("Failed to execute dtfu");
    world.output = Some(output);
}

#[then(regex = r#"^the command should succeed$"#)]
fn command_should_succeed(world: &mut CliWorld) {
    let output = world.output.as_ref().expect("No output captured");
    assert!(
        output.status.success(),
        "Command failed with exit code {:?}:\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[then(regex = r#"^the output should contain "(.+)"$"#)]
fn output_should_contain(world: &mut CliWorld, expected: String) {
    let output = world.output.as_ref().expect("No output captured");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    let expected_resolved = if let Some(ref temp_dir) = world.temp_dir {
        let temp_path = temp_dir
            .path()
            .to_str()
            .expect("Temp path is not valid UTF-8");
        replace_tempdir(&expected, temp_path)
    } else {
        expected
    };

    assert!(
        combined.contains(&expected_resolved),
        "Expected output to contain '{}', but got:\nstdout: {}\nstderr: {}",
        expected_resolved,
        stdout,
        stderr
    );
}

#[then(regex = r#"^the file "(.+)" should exist$"#)]
fn file_should_exist(world: &mut CliWorld, path: String) {
    let path_resolved = if let Some(ref temp_dir) = world.temp_dir {
        let temp_path = temp_dir
            .path()
            .to_str()
            .expect("Temp path is not valid UTF-8");
        replace_tempdir(&path, temp_path)
    } else {
        path
    };

    assert!(
        Path::new(&path_resolved).exists(),
        "Expected file to exist: {}",
        path_resolved
    );
}

fn main() {
    futures::executor::block_on(CliWorld::run("features"));
}
