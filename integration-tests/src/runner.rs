use std::{path::Path, process::Command};

fn run_command(cmd: &mut Command, what: &str) {
	let out = cmd.status().expect(&format!("failed to execute process {:?}", cmd));
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
				.arg("apply").arg(format!("./{}.patch", test.0))
				.current_dir(Path::new("./datafusion")),
			"apply git patch"
		);

		run_command(
			Command::new("git")
				.arg("reset").arg("--hard").arg("HEAD")
				.current_dir(Path::new("./datafusion")),
			"revert git patch"
		);
	}		
}
