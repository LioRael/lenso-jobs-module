use std::{collections::BTreeSet, fs, path::PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn repository_owns_only_the_jobs_capability_and_plugin() {
    let crates = fs::read_dir(repository_root().join("crates"))
        .expect("read repository crates")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().join("Cargo.toml").is_file())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<BTreeSet<_>>();
    let expected = ["lenso-capability-jobs", "lenso-jobs-plugin"]
        .into_iter()
        .map(str::to_owned)
        .collect();

    assert_eq!(crates, expected);
}
