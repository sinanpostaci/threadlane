//! Select a correction source by its entry in the approved repository's Git registry.
use std::fs;
use std::path::{Path, PathBuf};
use threadlane_workers::run_sanitized_git_bytes;

const INVALID: &str = "Select a canonical registered detached worktree of the approved repository at the exact base commit.";
const PATH_LIMIT: usize = 4096;

fn git(dir: &Path, args: &[&str], limit: usize) -> Result<Vec<u8>, &'static str> {
    run_sanitized_git_bytes(dir, args, limit, 5000)
        .map(|out| out.stdout_bytes)
        .map_err(|_| INVALID)
}

fn git_path(dir: &Path, option: &str) -> Result<PathBuf, &'static str> {
    let bytes = git(
        dir,
        &["rev-parse", "--path-format=absolute", option],
        PATH_LIMIT + 1,
    )?;
    let text = std::str::from_utf8(&bytes).map_err(|_| INVALID)?;
    let path = Path::new(text.strip_suffix('\n').ok_or(INVALID)?);
    if !path.is_absolute() {
        return Err(INVALID);
    }
    Ok(path.to_owned())
}

// Git stores a backlink to the checkout, and a forward link to its admin directory.
// Treat them as bounded metadata, never as source files or instructions.
#[cfg(unix)]
fn link_line(path: &Path) -> Result<Vec<u8>, &'static str> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| INVALID)?;
    if !file.metadata().map_err(|_| INVALID)?.is_file() {
        return Err(INVALID);
    }
    let mut bytes = Vec::new();
    file.take((PATH_LIMIT + 10) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| INVALID)?;
    if bytes.len() >= PATH_LIMIT + 10 || bytes.pop() != Some(b'\n') {
        return Err(INVALID);
    }
    Ok(bytes)
}

#[cfg(not(unix))]
fn link_line(_: &Path) -> Result<Vec<u8>, &'static str> {
    Err(INVALID)
}

pub(super) fn verify(repo: &Path, base: &str, selected: &Path) -> Result<(), &'static str> {
    let name = selected.to_str().ok_or(INVALID)?;
    if !selected.is_absolute()
        || selected == repo
        || name.len() > PATH_LIMIT
        || name.chars().any(char::is_control)
    {
        return Err(INVALID);
    }
    let registry = git(repo, &["worktree", "list", "--porcelain", "-z"], 128 * 1024)?;

    let head_field = format!("HEAD {base}");
    let mut eligible = false;
    let mut fields = registry
        .strip_suffix(b"\0\0")
        .ok_or(INVALID)?
        .split(|&c| c == 0);
    while let Some(header) = fields.next() {
        let path = header.strip_prefix(b"worktree ").ok_or(INVALID)?;
        let details = fields.by_ref().take_while(|field| !field.is_empty());
        if path != name.as_bytes() {
            details.for_each(drop);
            continue;
        }
        let mut head = false;
        let mut detached = false;
        for field in details {
            if field.starts_with(b"HEAD ") {
                if head || field != head_field.as_bytes() {
                    return Err(INVALID);
                }
                head = true;
            } else if field == b"detached" {
                detached = true;
            } else if field != b"locked" && !field.starts_with(b"locked ") {
                return Err(INVALID);
            }
        }
        if eligible || !head || !detached {
            return Err(INVALID);
        }
        eligible = true;
    }
    if !eligible
        || fs::canonicalize(selected).map_err(|_| INVALID)?.as_os_str() != selected.as_os_str()
    {
        return Err(INVALID);
    }

    let common = git_path(repo, "--git-common-dir")?
        .canonicalize()
        .map_err(|_| INVALID)?;
    let checkout_link = selected.join(".git");
    let forward = link_line(&checkout_link)?;
    let target = std::str::from_utf8(forward.strip_prefix(b"gitdir: ").ok_or(INVALID)?)
        .map_err(|_| INVALID)?;
    let admin = selected.join(target).canonicalize().map_err(|_| INVALID)?;
    if admin.parent() != Some(common.join("worktrees").as_path()) {
        return Err(INVALID);
    }
    let backlink = link_line(&admin.join("gitdir"))?;
    let back = std::str::from_utf8(&backlink).map_err(|_| INVALID)?;
    if admin.join(back).canonicalize().map_err(|_| INVALID)? != checkout_link {
        return Err(INVALID);
    }
    if git_path(selected, "--show-toplevel")? != selected
        || git_path(selected, "--absolute-git-dir")?
            .canonicalize()
            .map_err(|_| INVALID)?
            != admin
        || git_path(selected, "--git-common-dir")?
            .canonicalize()
            .map_err(|_| INVALID)?
            != common
        || git(selected, &["rev-parse", "--verify", "HEAD"], 128)?.strip_suffix(b"\n")
            != Some(base.as_bytes())
    {
        return Err(INVALID);
    }
    Ok(())
}
