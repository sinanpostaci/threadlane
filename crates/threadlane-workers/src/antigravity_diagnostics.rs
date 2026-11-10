//! Bounded classification of unverified AGY diagnostic text from invocation-owned capture.
//!
//! Qualified AGY 1.5.3 startup opens private capture/capture.ansi FIFOs; this does not prove semantic
//! per-line origin. An exact log-shaped forgery can produce only an unverified advisory hint.
use crate::provider_health::{
    initial_snapshot, set_coverage, ProviderDiagnosticAvailability as Availability,
    ProviderDiagnosticUnavailableReason as Unavailable, ProviderHealthCategory,
    ProviderHealthObservation, ProviderHealthSnapshot, MAX_PROVIDER_HEALTH_ELAPSED_MS,
    MAX_PROVIDER_HEALTH_RECORDS,
};

/// Combined raw-byte classification budget for the invocation's log and companion.
pub const MAX_DIAGNOSTIC_BYTES: usize = 1_048_576;
/// Maximum pending line bytes, including an unterminated fragment.
// Qualified startup contained a 4173-byte harmless line before the first retry.
// Keep finite headroom within the unchanged combined 1 MiB classification budget.
pub const MAX_DIAGNOSTIC_LINE_BYTES: usize = 16 * 1024;

/// Incremental classifier with explicit owned-source byte positions and a fake-clock seam.
///
/// Call only with the invocation-owned diagnostic source. Ordinary stdout, stderr, model prose
/// and tool output are never inputs to this classifier. Bytes and logger timestamps are not
/// retained. A caller must independently verify ownership; content always remains untrusted.
pub struct AntigravityDiagnosticParser {
    snapshot: ProviderHealthSnapshot,
    position: u64,
    bytes_seen: usize,
    records_seen: u32,
    pending: Vec<u8>,
}

impl Default for AntigravityDiagnosticParser {
    fn default() -> Self {
        Self::new()
    }
}

impl AntigravityDiagnosticParser {
    /// Starts a new parser for exactly one separately owned source, never a shared log.
    pub fn new() -> Self {
        let mut snapshot = initial_snapshot(true);
        set_coverage(&mut snapshot, Availability::Available, None, 0);
        Self {
            snapshot,
            position: 0,
            bytes_seen: 0,
            records_seen: 0,
            pending: Vec::with_capacity(MAX_DIAGNOSTIC_LINE_BYTES),
        }
    }

    /// Latest cumulative allowlisted metadata, without any raw text or source path.
    pub fn snapshot(&self) -> ProviderHealthSnapshot {
        self.snapshot.clone()
    }

    /// Observes bytes at their owned-source offset; rereads cannot create duplicate retries.
    ///
    /// A gap loses source continuity. Monotonic time is supplied by supervision or a fake clock;
    /// regressing inputs are clamped. Resource exhaustion stops classification, not execution.
    pub fn ingest_at(&mut self, offset: u64, bytes: &[u8], elapsed_ms: u64) {
        if self.snapshot.availability != Availability::Available {
            return;
        }
        if offset > self.position {
            self.unavailable(Unavailable::SourceLost, elapsed_ms);
            return;
        }
        let overlap = self.position - offset;
        if overlap >= bytes.len() as u64 {
            return;
        }
        let fresh = &bytes[overlap as usize..];
        for &byte in fresh {
            if self.bytes_seen == MAX_DIAGNOSTIC_BYTES {
                self.truncated(elapsed_ms);
                return;
            }
            self.bytes_seen += 1;
            self.position += 1;
            if byte == b'\n' {
                self.records_seen += 1;
                if let Some(attempt) = classify_retry(&self.pending) {
                    self.observe_retry(attempt, elapsed_ms);
                }
                self.pending.clear();
                if self.records_seen == MAX_PROVIDER_HEALTH_RECORDS {
                    self.truncated(elapsed_ms);
                    return;
                }
            } else if self.pending.len() == MAX_DIAGNOSTIC_LINE_BYTES {
                self.truncated(elapsed_ms);
                return;
            } else {
                self.pending.push(byte);
            }
        }
        if self.bytes_seen == MAX_DIAGNOSTIC_BYTES {
            self.truncated(elapsed_ms);
        }
    }

    /// Marks resource exhaustion and erases the incomplete raw fragment.
    pub fn truncated(&mut self, elapsed_ms: u64) {
        set_coverage(
            &mut self.snapshot,
            Availability::Truncated,
            Some(Unavailable::ResourceLimit),
            elapsed_ms,
        );
        self.pending.clear();
    }

    /// Marks lost/closed capture without inferring recovery from silence or an incomplete line.
    pub fn unavailable(&mut self, reason: Unavailable, elapsed_ms: u64) {
        if reason == Unavailable::ResourceLimit {
            self.truncated(elapsed_ms);
            return;
        }
        if self.snapshot.availability != Availability::Truncated
            && self.snapshot.unavailable_reason != Some(Unavailable::SourceLost)
        {
            set_coverage(
                &mut self.snapshot,
                Availability::Unavailable,
                Some(reason),
                elapsed_ms,
            );
        }
        self.pending.clear();
    }

    fn observe_retry(&mut self, attempt: u32, elapsed_ms: u64) {
        self.snapshot.elapsed_ms = elapsed_ms
            .max(self.snapshot.elapsed_ms)
            .min(MAX_PROVIDER_HEALTH_ELAPSED_MS);
        self.snapshot.update_sequence += 1;
        self.snapshot.retry_count += 1;
        self.snapshot.retry_high_water =
            Some(self.snapshot.retry_high_water.unwrap_or(0).max(attempt));
        let observation = ProviderHealthObservation {
            category: ProviderHealthCategory::StreamIdleRetry,
            elapsed_ms: self.snapshot.elapsed_ms,
            update_sequence: self.snapshot.update_sequence,
        };
        self.snapshot.first_failure.get_or_insert(observation);
        self.snapshot.last_failure = Some(observation);
        self.snapshot.current_failure = Some(observation.category);
        self.snapshot.recovery = None;
    }
}

fn digits(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|b| b.is_ascii_digit())
}

fn classify_retry(line: &[u8]) -> Option<u32> {
    // Deliberately narrow observed grammar. No JSON, substring search, ANSI stripping, auth
    // classification, root-cause claim, or invented recovery grammar is accepted.
    let line = std::str::from_utf8(line).ok()?;
    let (prefix, body) = line.split_once("] ")?;
    let (date, rest) = prefix.split_once(' ')?;
    let (time, rest) = rest.split_once(' ')?;
    // The observed logger right-aligns the PID. Permit only ASCII space padding
    // in that position; do not normalize arbitrary whitespace, prose or source labels.
    let (pid, source) = rest.trim_start_matches(' ').split_once(' ')?;
    if date.len() != 5
        || !date.starts_with('I')
        || !digits(&date[1..], 4)
        || time.len() != 15
        || !time.is_ascii()
        || &time[2..3] != ":"
        || &time[5..6] != ":"
        || &time[8..9] != "."
        || !digits(&time[..2], 2)
        || !digits(&time[3..5], 2)
        || !digits(&time[6..8], 2)
        || !digits(&time[9..], 6)
        || pid.is_empty()
        || pid.len() > 10
        || !pid.bytes().all(|b| b.is_ascii_digit())
        || source != "run.go:315"
    {
        return None;
    }
    let body = body.strip_prefix("Run: attempt ")?;
    let (attempt, body) = body.split_once(" failed (")?;
    if attempt.starts_with('0') || attempt.len() > 4 {
        return None;
    }
    let attempt: u32 = attempt.parse().ok()?;
    if !(1..=MAX_PROVIDER_HEALTH_RECORDS).contains(&attempt) {
        return None;
    }
    let (error, delay) = body.split_once("), retrying in ")?;
    if !matches!(
        error,
        "stream reading error: stream idle timeout (5m0s): context deadline exceeded"
            | "request failed: stream idle timeout (5m0s): context deadline exceeded"
    ) {
        return None;
    }
    let delay = delay.strip_suffix('.').unwrap_or(delay).strip_suffix('s')?;
    let (seconds, fraction) = delay
        .split_once('.')
        .map_or((delay, None), |(s, f)| (s, Some(f)));
    if seconds.is_empty()
        || seconds.len() > 3
        || !seconds.bytes().all(|b| b.is_ascii_digit())
        || seconds.parse::<u32>().ok()? > 600
        || fraction
            .is_some_and(|f| f.is_empty() || f.len() > 9 || !f.bytes().all(|b| b.is_ascii_digit()))
    {
        return None;
    }
    Some(attempt)
}

use crate::provider_health::HealthPublisher;
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub(crate) struct DiagnosticTask {
    stop: watch::Sender<bool>,
    handle: Option<tokio::task::JoinHandle<()>>,
    publisher: HealthPublisher,
}

impl DiagnosticTask {
    pub(crate) async fn finish(&mut self, elapsed_ms: u64) {
        self.stop.send_replace(true);
        if let Some(mut handle) = self.handle.take() {
            match tokio::time::timeout(Duration::from_millis(100), &mut handle).await {
                Ok(Ok(())) => {}
                _ => {
                    handle.abort();
                    let mut snapshot = self.publisher.snapshot();
                    if snapshot.availability != Availability::Truncated {
                        set_coverage(
                            &mut snapshot,
                            Availability::Unavailable,
                            Some(Unavailable::SourceLost),
                            elapsed_ms,
                        );
                    }
                    self.publisher.publish(snapshot);
                }
            }
        }
    }
}

impl Drop for DiagnosticTask {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pub(crate) fn start_owned_capture(
    supported: bool,
    argv: &mut Vec<String>,
    publisher: HealthPublisher,
    started: Instant,
) -> Option<DiagnosticTask> {
    if !supported {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        let capture = capture::OwnedCapture::new().ok()?;
        let path = capture.log_path().to_str()?.to_owned();
        // Only software creates this path; the report's exact argv is the sole path-bearing output.
        drop(argv.splice(0..0, ["--log-file".to_owned(), path]));
        let (stop, mut stop_rx) = watch::channel(false);
        let task_publisher = publisher.clone();
        let handle = tokio::spawn(async move {
            let mut capture = capture;
            let mut parser = AntigravityDiagnosticParser::new();
            task_publisher.publish(parser.snapshot());
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let stopping = tokio::select! {
                    biased;
                    () = crate::supervisor::wait_for_cancellation(&mut stop_rx) => true,
                    _ = interval.tick() => false,
                };
                let elapsed = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                capture.drain_round(&mut parser, elapsed);
                if stopping {
                    parser.unavailable(Unavailable::SourceClosed, elapsed);
                }
                task_publisher.publish(parser.snapshot());
                if stopping {
                    break;
                }
            }
        });
        Some(DiagnosticTask {
            stop,
            handle: Some(handle),
            publisher,
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (argv, publisher, started);
        None
    }
}

#[cfg(target_os = "linux")]
mod capture {
    use super::*;
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Read};
    use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::path::{Path, PathBuf};

    pub(super) struct OwnedCapture {
        directory: PathBuf,
        directory_file: File,
        parent_file: File,
        directory_name: std::ffi::OsString,
        log_path: PathBuf,
        log: File,
        ansi: File,
        bytes_seen: usize,
        log_offset: u64,
    }

    fn same_object(a: &fs::Metadata, b: &fs::Metadata) -> bool {
        a.dev() == b.dev() && a.ino() == b.ino() && a.uid() == b.uid()
    }

    impl OwnedCapture {
        pub(super) fn new() -> io::Result<Self> {
            let directory = tempfile::Builder::new()
                .prefix("threadlane-agy-diagnostic-")
                .permissions(fs::Permissions::from_mode(0o700))
                .tempdir()?;
            let canonical = crate::fs_safety::validate_existing_canonical_dir(
                directory.path(),
                "diagnostic capture",
            )
            .map_err(|_| io::Error::other("unavailable diagnostic directory"))?;
            let directory_file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&canonical)?;
            let parent_file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(
                    canonical
                        .parent()
                        .ok_or_else(|| io::Error::other("missing parent"))?,
                )?;
            let directory_name = canonical
                .file_name()
                .ok_or_else(|| io::Error::other("missing directory name"))?
                .to_owned();
            let log_path = canonical.join("capture");
            let ansi_path = canonical.join("capture.ansi");
            let log = crate::fs_safety::create_private_diagnostic_fifo(&log_path)?;
            let ansi = crate::fs_safety::create_private_diagnostic_fifo(&ansi_path)?;
            // From here cleanup removes only our exact still-owned FIFO identities, never a
            // recursive tree potentially changed by the worker.
            let this = Self {
                directory: directory.keep(),
                directory_file,
                parent_file,
                directory_name,
                log_path,
                log,
                ansi,
                bytes_seen: 0,
                log_offset: 0,
            };
            if !this.continuity() {
                return Err(io::Error::other("unavailable diagnostic identity"));
            }
            Ok(this)
        }

        pub(super) fn log_path(&self) -> &Path {
            &self.log_path
        }

        fn fifo_matches(&self, file: &File, path: &Path) -> bool {
            let (Ok(held), Ok(named), Ok(dir)) = (
                file.metadata(),
                fs::symlink_metadata(path),
                self.directory_file.metadata(),
            ) else {
                return false;
            };
            same_object(&held, &named)
                && named.file_type().is_fifo()
                && named.mode() & 0o7777 == 0o600
                && named.nlink() == 1
                && named.uid() == dir.uid()
        }

        fn continuity(&self) -> bool {
            let (Ok(held), Ok(named)) = (
                self.directory_file.metadata(),
                fs::symlink_metadata(&self.directory),
            ) else {
                return false;
            };
            same_object(&held, &named)
                && named.file_type().is_dir()
                && named.mode() & 0o7777 == 0o700
                && self.fifo_matches(&self.log, &self.log_path)
                && self.fifo_matches(&self.ansi, &self.directory.join("capture.ansi"))
        }

        pub(super) fn drain_round(
            &mut self,
            parser: &mut AntigravityDiagnosticParser,
            elapsed: u64,
        ) {
            let intact = self.continuity();
            if !intact {
                parser.unavailable(Unavailable::SourceLost, elapsed);
            }
            let mut chunk = [0u8; 4096];
            // Fixed syscall/CPU work per round. Both sources use 4096-byte kernel FIFOs:
            // no regular raw log exists on disk, even after the classification budget ends.
            for companion in [false, true] {
                for _ in 0..4 {
                    let read = if companion {
                        self.ansi.read(&mut chunk)
                    } else {
                        self.log.read(&mut chunk)
                    };
                    match read {
                        Ok(0) => break,
                        Ok(n) => {
                            let allowed =
                                MAX_DIAGNOSTIC_BYTES.saturating_sub(self.bytes_seen).min(n);
                            self.bytes_seen =
                                self.bytes_seen.saturating_add(n).min(MAX_DIAGNOSTIC_BYTES);
                            if intact && !companion && allowed > 0 {
                                parser.ingest_at(self.log_offset, &chunk[..allowed], elapsed);
                            }
                            if !companion {
                                self.log_offset = self.log_offset.saturating_add(n as u64);
                            }
                            if self.bytes_seen == MAX_DIAGNOSTIC_BYTES {
                                parser.truncated(elapsed);
                            }
                            // After truncation/loss we keep discarding at a bounded rate so
                            // diagnostics do not create a cancellation or output-overflow policy.
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(_) => {
                            parser.unavailable(Unavailable::SourceLost, elapsed);
                            break;
                        }
                    }
                }
            }
        }
    }

    impl Drop for OwnedCapture {
        fn drop(&mut self) {
            if self.continuity() {
                // Descriptor-relative lookups cannot follow replaced directory parents.
                // This is not an OS sandbox against arbitrary same-user writes.
                let log_removed = crate::fs_safety::remove_owned_diagnostic_entry(
                    &self.directory_file,
                    std::ffi::OsStr::new("capture"),
                    &self.log,
                    false,
                );
                let ansi_removed = crate::fs_safety::remove_owned_diagnostic_entry(
                    &self.directory_file,
                    std::ffi::OsStr::new("capture.ansi"),
                    &self.ansi,
                    false,
                );
                if log_removed.is_ok() && ansi_removed.is_ok() {
                    // Non-recursive: preserve unexpected entries.
                    let _ = crate::fs_safety::remove_owned_diagnostic_entry(
                        &self.parent_file,
                        &self.directory_name,
                        &self.directory_file,
                        true,
                    );
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::Write;
        use std::os::unix::fs::symlink;

        #[test]
        fn renamed_parent_symlink_cannot_redirect_cleanup_to_replacement_entries() {
            let capture = OwnedCapture::new().unwrap();
            let original = capture.directory.clone();
            let relocated = original.with_extension("relocated");
            fs::rename(&original, &relocated).unwrap();
            let replacement = tempfile::tempdir().unwrap();
            let capture_sentinel = replacement.path().join("capture");
            let ansi_sentinel = replacement.path().join("capture.ansi");
            fs::write(&capture_sentinel, b"preserve unrelated log").unwrap();
            fs::write(&ansi_sentinel, b"preserve unrelated companion").unwrap();
            symlink(replacement.path(), &original).unwrap();
            drop(capture);
            assert_eq!(
                fs::read(&capture_sentinel).unwrap(),
                b"preserve unrelated log"
            );
            assert_eq!(
                fs::read(&ansi_sentinel).unwrap(),
                b"preserve unrelated companion"
            );
            // Questionable moved objects stay intact rather than triggering recursive cleanup.
            assert!(relocated.join("capture").exists());
            fs::remove_file(&original).unwrap();
            fs::remove_file(relocated.join("capture")).unwrap();
            fs::remove_file(relocated.join("capture.ansi")).unwrap();
            fs::remove_dir(relocated).unwrap();
        }

        #[test]
        fn private_bounded_pair_discards_companion_and_refuses_replacement() {
            let mut capture = OwnedCapture::new().unwrap();
            let directory = capture.directory.clone();
            assert_eq!(fs::metadata(&directory).unwrap().mode() & 0o777, 0o700);
            assert_eq!(
                fs::metadata(capture.log_path()).unwrap().mode() & 0o777,
                0o600
            );
            let mut parser = AntigravityDiagnosticParser::new();
            let mut writer = OpenOptions::new()
                .write(true)
                .open(capture.log_path())
                .unwrap();
            writer.write_all(b"I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s\n").unwrap();
            capture.drain_round(&mut parser, 7);
            assert_eq!(parser.snapshot().retry_count, 1);
            drop(writer);
            let ansi_path = directory.join("capture.ansi");
            let mut ansi = OpenOptions::new().write(true).open(&ansi_path).unwrap();
            for _ in 0..(MAX_DIAGNOSTIC_BYTES / 4096) {
                ansi.write_all(&[b'x'; 4096]).unwrap();
                capture.drain_round(&mut parser, 8);
            }
            assert_eq!(parser.snapshot().availability, Availability::Truncated);
            drop(ansi);
            drop(capture);
            assert!(!directory.exists());

            let mut capture = OwnedCapture::new().unwrap();
            let directory = capture.directory.clone();
            let log_path = capture.log_path.clone();
            let outside = tempfile::NamedTempFile::new().unwrap();
            fs::remove_file(&log_path).unwrap();
            symlink(outside.path(), &log_path).unwrap();
            let mut parser = AntigravityDiagnosticParser::new();
            capture.drain_round(&mut parser, 9);
            assert_eq!(
                parser.snapshot().unavailable_reason,
                Some(Unavailable::SourceLost)
            );
            drop(capture);
            assert!(outside.path().exists());
            fs::remove_file(log_path).unwrap();
            fs::remove_file(directory.join("capture.ansi")).unwrap();
            fs::remove_dir(directory).unwrap();
        }
    }
}
