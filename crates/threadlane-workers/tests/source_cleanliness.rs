use std::fs;
use tempfile::TempDir;
use threadlane_workers::{
    is_source_worktree_clean, is_worktree_clean, run_sanitized_git, DEFAULT_GIT_COMMAND_TIMEOUT_MS,
};

fn setup_repo_with_ignored_files() -> TempDir {
    let temp = TempDir::new().expect("create temp dir");
    let repo = temp.path();
    run_sanitized_git(repo, &["init"], DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("git init");
    run_sanitized_git(
        repo,
        &["config", "user.name", "Threadlane Test"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git config user.name");
    run_sanitized_git(
        repo,
        &["config", "user.email", "test@threadlane.local"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git config user.email");

    fs::create_dir_all(repo.join("src")).expect("create src dir");
    fs::write(
        repo.join("src/lib.rs"),
        "pub fn answer() -> u32 {\n    42\n}\n",
    )
    .expect("write src/lib.rs");
    fs::write(repo.join(".gitignore"), "target/\n.env\n").expect("write .gitignore");

    run_sanitized_git(
        repo,
        &["add", ".gitignore", "src/lib.rs"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git add initial files");
    run_sanitized_git(
        repo,
        &["commit", "-m", "initial commit"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("git commit initial files");

    fs::create_dir_all(repo.join("target")).expect("create target dir");
    fs::write(repo.join("target/cache.bin"), b"cached-build-output")
        .expect("write ignored target file");
    fs::write(repo.join(".env"), "LOCAL_ONLY=1\n").expect("write ignored .env file");
    temp
}

#[test]
fn source_worktree_clean_allows_ignored_files_while_strict_helper_rejects_them() {
    let temp = setup_repo_with_ignored_files();
    let repo = temp.path();

    assert!(
        is_source_worktree_clean(repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("check source cleanliness")
    );
    assert!(
        !is_worktree_clean(repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS).expect("check strict cleanliness")
    );
    assert_eq!(
        fs::read(repo.join("target/cache.bin")).expect("read target/cache.bin"),
        b"cached-build-output"
    );
    assert_eq!(
        fs::read_to_string(repo.join(".env")).expect("read .env"),
        "LOCAL_ONLY=1\n"
    );
}

#[test]
fn source_worktree_clean_rejects_tracked_staged_and_untracked_changes() {
    let tracked_temp = setup_repo_with_ignored_files();
    let tracked_repo = tracked_temp.path();
    fs::write(
        tracked_repo.join("src/lib.rs"),
        "pub fn answer() -> u32 {\n    43\n}\n",
    )
    .expect("modify tracked file");
    assert!(
        !is_source_worktree_clean(tracked_repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("check tracked modification")
    );

    let staged_temp = setup_repo_with_ignored_files();
    let staged_repo = staged_temp.path();
    fs::write(
        staged_repo.join("src/lib.rs"),
        "pub fn answer() -> u32 {\n    44\n}\n",
    )
    .expect("modify file for staging");
    run_sanitized_git(
        staged_repo,
        &["add", "src/lib.rs"],
        DEFAULT_GIT_COMMAND_TIMEOUT_MS,
    )
    .expect("stage modified file");
    assert!(
        !is_source_worktree_clean(staged_repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("check staged modification")
    );

    let untracked_temp = setup_repo_with_ignored_files();
    let untracked_repo = untracked_temp.path();
    fs::write(untracked_repo.join("untracked.txt"), "scratch\n").expect("write untracked file");
    assert!(
        !is_source_worktree_clean(untracked_repo, DEFAULT_GIT_COMMAND_TIMEOUT_MS)
            .expect("check non-ignored untracked file")
    );
}
