use std::{path::Path, process::{Command, Stdio}};

fn run_command(cmd: &mut Command, what: &str) {
	let out = cmd.stdout(Stdio::null()).stderr(Stdio::null()).status()
		.expect(&format!("failed to execute process intended to {:?}", cmd));
	if !out.success() {
		panic!("failed to {}", what)
	}
}

fn main() {
	let tests = vec![
		("aggressive_filter_est", 0),
		("bad_join_sel", 0),
	];

	for test in tests {
		run_command(
			Command::new("git")
				.arg("apply").arg(format!("../{}.patch", test.0))
				.current_dir(Path::new("./datafusion")),
			"apply git patch"
		);

		println!("Compiling with patch '{}'...", test.0);
		run_command(
			Command::new("cargo")
				.arg("build").arg("--bin").arg("test"),
			"rebuild test script"
		);

		println!("Running test with patch '{}'...", test.0);
		let output = Command::new("./target/debug/test")
			.output().expect("failed to execute test");
		if !output.status.success() {
			panic!("Test '{}' failed.", test.0);
		}
		let output = String::from_utf8(output.stdout)
			.expect("bad UTF-8 in test output");
		println!("{output}");
		
		run_command(
			Command::new("git")
				.arg("reset").arg("--hard").arg("HEAD")
				.current_dir(Path::new("./datafusion")),
			"revert git patch"
		);
	}		
}
