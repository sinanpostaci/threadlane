#![deny(unsafe_code)]
#![deny(missing_docs)]

//! Packaged trusted local operator context router (`threadlane-context`) for native Codex workflows.
//!
//! Validates a private (`0600`) version-1 operator context JSON file, verifies the bound
//! `(binding_id, thread_uuid)` target against the live authenticated local supervisor, and
//! either emits a bounded `check` verification envelope or invokes the canonical sibling
//! `threadlane-workflow`, `threadlane-session`, or `threadlane-flow` binary directly via `argv`
//! (never a shell).
//!
//! ## Security and Authority Boundaries
//!
//! - **Target selection, not native caller identity or human approval**: Validating a context file
//!   and live binding confirms that the selected target exists in the running supervisor. It does
//!   not authenticate per-call native chat provenance (`native_per_call_identity_verified: false`)
//!   or grant human scope approval (`human_approval_verified: false`).
//! - **Package pin only**: Legacy installation metadata (such as `cli_package`, `desktop_package`,
//!   and XDG fields) is never granted authority to choose executables or override bound stores.
//!   When `cli_package` is present, its `bin` directory must match the current executable's
//!   canonical sibling directory.
//! - **Canonical sibling execution only**: `workflow`, `session`, and `flow` resolve only the
//!   canonical `threadlane-workflow`, `threadlane-session`, or `threadlane-flow` regular executable
//!   in the current binary's canonical parent directory, rejecting missing, symlink, non-regular,
//!   or non-executable targets.
//! - **Context-free family help**: `help workflow`, `help session`, and `help flow` execute only the
//!   canonical same-package sibling with a single fixed `--help` argument, without reading a
//!   context, validating stores, or contacting a supervisor.
//! - **Locked scope flags**: Bound store directories, `--binding`, `--thread`, and `--context`
//!   come exclusively from the private context file; forwarded overrides (including `=` forms and
//!   repeated flags) are rejected before child execution.

use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use serde::de::{self, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use threadlane_native::fs_safety::ensure_platform_supported;
use threadlane_native::operator_cli::{
    emit_error, read_request, validate_dirs, verify_binding, write_result, OperatorCliError,
};

const CONTEXT_SCHEMA_VERSION: u32 = 1;
const TRUSTED_LOCAL_OPERATOR_ROUTE: &str = "trusted_local_operator";
const WORKFLOW_BINARY_NAME: &str = "threadlane-workflow";
const SESSION_BINARY_NAME: &str = "threadlane-session";
const FLOW_BINARY_NAME: &str = "threadlane-flow";

const OUTER_HELP_TEXT: &str = "\
threadlane-context — Packaged trusted local operator context CLI for Threadlane

USAGE:
    threadlane-context --context <ABS_PRIVATE_JSON> check
    threadlane-context --context <ABS_PRIVATE_JSON> workflow <WORKFLOW_ARGS>...
    threadlane-context --context <ABS_PRIVATE_JSON> session <SESSION_ARGS>...
    threadlane-context --context <ABS_PRIVATE_JSON> flow <FLOW_ARGS>...
    threadlane-context help workflow
    threadlane-context help session
    threadlane-context help flow
    threadlane-context --help | -h | help
    threadlane-context --version | -V

FAMILIES:
    check       Validate private v1 context schema, project root, private stores, package pin, and live binding
    workflow    Invoke canonical sibling threadlane-workflow with context-bound scope flags
    session     Invoke canonical sibling threadlane-session with context-bound scope flags
    flow        Invoke canonical sibling threadlane-flow with the same five stores, binding, and thread as session
    help        Display outer help, or context-free sibling help for workflow/session/flow
";

const PROTECTED_OVERRIDE_FLAGS: &[&str] = &[
    "--state-dir",
    "--state_dir",
    "--registry-dir",
    "--registry_dir",
    "--plans-dir",
    "--plans_dir",
    "--authority-dir",
    "--authority_dir",
    "--sessions-dir",
    "--sessions_dir",
    "--binding",
    "--binding-id",
    "--binding_id",
    "--thread",
    "--thread-uuid",
    "--thread_uuid",
    "--context",
    "--project-root",
    "--project_root",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildFamily {
    Workflow,
    Session,
    Flow,
}

impl ChildFamily {
    const fn binary_name(self) -> &'static str {
        match self {
            Self::Workflow => WORKFLOW_BINARY_NAME,
            Self::Session => SESSION_BINARY_NAME,
            Self::Flow => FLOW_BINARY_NAME,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ContextCommand {
    Check,
    Forward {
        family: ChildFamily,
        inner_args: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OuterParseAction {
    DisplayHelp,
    DisplayVersion,
    DisplaySiblingHelp {
        family: ChildFamily,
    },
    Execute {
        context_path: PathBuf,
        command: ContextCommand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawContextStores {
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
}

impl<'de> Deserialize<'de> for RawContextStores {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StoresVisitor;

        impl<'de> Visitor<'de> for StoresVisitor {
            type Value = RawContextStores;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a version-1 context stores object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen_keys = BTreeSet::new();
                let mut state_dir: Option<PathBuf> = None;
                let mut registry_dir: Option<PathBuf> = None;
                let mut plans_dir: Option<PathBuf> = None;
                let mut authority_dir: Option<PathBuf> = None;
                let mut sessions_dir: Option<PathBuf> = None;

                while let Some(key) = map.next_key::<String>()? {
                    if !seen_keys.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate field in context stores"));
                    }
                    match key.as_str() {
                        "state_dir" => {
                            state_dir = Some(map.next_value()?);
                        }
                        "registry_dir" => {
                            registry_dir = Some(map.next_value()?);
                        }
                        "plans_dir" => {
                            plans_dir = Some(map.next_value()?);
                        }
                        "authority_dir" => {
                            authority_dir = Some(map.next_value()?);
                        }
                        "sessions_dir" => {
                            sessions_dir = Some(map.next_value()?);
                        }
                        other => {
                            return Err(de::Error::unknown_field(
                                other,
                                &[
                                    "state_dir",
                                    "registry_dir",
                                    "plans_dir",
                                    "authority_dir",
                                    "sessions_dir",
                                ],
                            ));
                        }
                    }
                }

                Ok(RawContextStores {
                    state_dir: state_dir.ok_or_else(|| de::Error::missing_field("state_dir"))?,
                    registry_dir: registry_dir
                        .ok_or_else(|| de::Error::missing_field("registry_dir"))?,
                    plans_dir: plans_dir.ok_or_else(|| de::Error::missing_field("plans_dir"))?,
                    authority_dir: authority_dir
                        .ok_or_else(|| de::Error::missing_field("authority_dir"))?,
                    sessions_dir: sessions_dir
                        .ok_or_else(|| de::Error::missing_field("sessions_dir"))?,
                })
            }
        }

        deserializer.deserialize_map(StoresVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawPrivateContext {
    schema_version: u32,
    project_root: PathBuf,
    binding_id: String,
    thread_uuid: String,
    route: String,
    native_per_call_identity_verified: bool,
    stores: RawContextStores,
    cli_package: Option<PathBuf>,
}

impl<'de> Deserialize<'de> for RawPrivateContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ContextVisitor;

        impl<'de> Visitor<'de> for ContextVisitor {
            type Value = RawPrivateContext;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a version-1 private Threadlane operator context object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut seen_keys = BTreeSet::new();
                let mut schema_version: Option<u32> = None;
                let mut project_root: Option<PathBuf> = None;
                let mut binding_id: Option<String> = None;
                let mut thread_uuid: Option<String> = None;
                let mut route: Option<String> = None;
                let mut native_per_call_identity_verified: Option<bool> = None;
                let mut stores: Option<RawContextStores> = None;
                let mut cli_package: Option<PathBuf> = None;

                while let Some(key) = map.next_key::<String>()? {
                    if !seen_keys.insert(key.clone()) {
                        return Err(de::Error::custom("duplicate field in private context"));
                    }
                    match key.as_str() {
                        "schema_version" | "version" => {
                            if schema_version.is_some() {
                                return Err(de::Error::duplicate_field("schema_version"));
                            }
                            schema_version = Some(map.next_value()?);
                        }
                        "project_root" => {
                            project_root = Some(map.next_value()?);
                        }
                        "binding_id" => {
                            binding_id = Some(map.next_value()?);
                        }
                        "thread_uuid" => {
                            thread_uuid = Some(map.next_value()?);
                        }
                        "route" => {
                            route = Some(map.next_value()?);
                        }
                        "native_per_call_identity_verified" => {
                            native_per_call_identity_verified = Some(map.next_value()?);
                        }
                        "stores" => {
                            stores = Some(map.next_value()?);
                        }
                        "cli_package" => {
                            cli_package = Some(map.next_value()?);
                        }
                        // Forbidden top-level store or target aliases that could conflict with
                        // operational fields fail closed rather than being ignored as metadata.
                        "state_dir" | "registry_dir" | "plans_dir" | "authority_dir"
                        | "sessions_dir" | "binding" | "thread" => {
                            return Err(de::Error::custom(
                                "top-level store or target override field is not permitted",
                            ));
                        }
                        // Legacy non-operational installation metadata (such as desktop_package
                        // and XDG fields) and unknown non-operational fields are ignored without
                        // granting authority.
                        _ => {
                            let _ = map.next_value::<IgnoredAny>()?;
                        }
                    }
                }

                Ok(RawPrivateContext {
                    schema_version: schema_version
                        .ok_or_else(|| de::Error::missing_field("schema_version"))?,
                    project_root: project_root
                        .ok_or_else(|| de::Error::missing_field("project_root"))?,
                    binding_id: binding_id.ok_or_else(|| de::Error::missing_field("binding_id"))?,
                    thread_uuid: thread_uuid
                        .ok_or_else(|| de::Error::missing_field("thread_uuid"))?,
                    route: route.ok_or_else(|| de::Error::missing_field("route"))?,
                    native_per_call_identity_verified: native_per_call_identity_verified
                        .ok_or_else(|| {
                            de::Error::missing_field("native_per_call_identity_verified")
                        })?,
                    stores: stores.ok_or_else(|| de::Error::missing_field("stores"))?,
                    cli_package,
                })
            }
        }

        deserializer.deserialize_map(ContextVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ValidatedStores {
    state_dir: PathBuf,
    registry_dir: PathBuf,
    plans_dir: PathBuf,
    authority_dir: PathBuf,
    sessions_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidatedContext {
    project_root: PathBuf,
    binding_id: String,
    thread_uuid: String,
    stores: ValidatedStores,
    current_bin_dir: PathBuf,
    cli_package_pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ContextCheckReport {
    schema_version: u32,
    project_root: PathBuf,
    binding_id: String,
    thread_uuid: String,
    route: &'static str,
    native_per_call_identity_verified: bool,
    live_binding_verified: bool,
    human_approval_verified: bool,
    cli_package_pinned: bool,
    stores: ValidatedStores,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let raw_args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let action = match parse_outer_args(&raw_args) {
        Ok(action) => action,
        Err(err) => {
            emit_error(&err);
            return ExitCode::from(1);
        }
    };

    match action {
        OuterParseAction::DisplayHelp => {
            let mut stdout = io::stdout().lock();
            if stdout.write_all(OUTER_HELP_TEXT.as_bytes()).is_err() || stdout.flush().is_err() {
                emit_error(&OperatorCliError::new(
                    "io_error",
                    "failed to write help output",
                ));
                return ExitCode::from(1);
            }
            ExitCode::SUCCESS
        }
        OuterParseAction::DisplayVersion => {
            let mut stdout = io::stdout().lock();
            let version_line = concat!("threadlane-context ", env!("CARGO_PKG_VERSION"), "\n");
            if stdout.write_all(version_line.as_bytes()).is_err() || stdout.flush().is_err() {
                emit_error(&OperatorCliError::new(
                    "io_error",
                    "failed to write version output",
                ));
                return ExitCode::from(1);
            }
            ExitCode::SUCCESS
        }
        OuterParseAction::DisplaySiblingHelp { family } => match run_sibling_help(family) {
            Ok(code) => code,
            Err(err) => {
                emit_error(&err);
                ExitCode::from(1)
            }
        },
        OuterParseAction::Execute {
            context_path,
            command,
        } => match run(&context_path, command).await {
            Ok(code) => code,
            Err(err) => {
                emit_error(&err);
                ExitCode::from(1)
            }
        },
    }
}

fn parse_outer_args(raw_args: &[OsString]) -> Result<OuterParseAction, OperatorCliError> {
    if raw_args.is_empty() {
        return Err(OperatorCliError::invalid_input(
            "invalid or missing command-line arguments",
        ));
    }

    let mut args = Vec::with_capacity(raw_args.len());
    for os_arg in raw_args {
        let Some(utf8) = os_arg.to_str() else {
            return Err(OperatorCliError::invalid_input(
                "command-line arguments must be valid UTF-8",
            ));
        };
        args.push(utf8);
    }

    if args.len() == 1 {
        match args[0] {
            "--help" | "-h" | "help" => return Ok(OuterParseAction::DisplayHelp),
            "--version" | "-V" => return Ok(OuterParseAction::DisplayVersion),
            _ => {
                return Err(OperatorCliError::invalid_input(
                    "invalid or missing command-line arguments",
                ));
            }
        }
    }

    if args[0] == "help" {
        if args.len() != 2 {
            return Err(OperatorCliError::invalid_input(
                "help command family accepts exactly one of workflow, session, or flow",
            ));
        }
        return match args[1] {
            "workflow" => Ok(OuterParseAction::DisplaySiblingHelp {
                family: ChildFamily::Workflow,
            }),
            "session" => Ok(OuterParseAction::DisplaySiblingHelp {
                family: ChildFamily::Session,
            }),
            "flow" => Ok(OuterParseAction::DisplaySiblingHelp {
                family: ChildFamily::Flow,
            }),
            _ => Err(OperatorCliError::invalid_input(
                "unsupported help family; only workflow, session, and flow are supported",
            )),
        };
    }

    let (context_raw, family_idx) = if args[0] == "--context" {
        if args.len() < 3 {
            return Err(OperatorCliError::invalid_input(
                "invalid or missing command-line arguments",
            ));
        }
        (args[1], 2usize)
    } else if let Some(eq_val) = args[0].strip_prefix("--context=") {
        if args.len() < 2 {
            return Err(OperatorCliError::invalid_input(
                "invalid or missing command-line arguments",
            ));
        }
        (eq_val, 1usize)
    } else {
        return Err(OperatorCliError::invalid_input(
            "expected `--context <ABS_PRIVATE_JSON>` before command family",
        ));
    };

    if context_raw.is_empty() {
        return Err(OperatorCliError::invalid_input(
            "context file path must not be empty",
        ));
    }

    let family_token = args[family_idx];
    if is_protected_override_token(family_token) {
        return Err(OperatorCliError::invalid_input(
            "protected scope or context flag overrides are not permitted",
        ));
    }

    let rest = &args[(family_idx + 1)..];
    let command = match family_token {
        "check" => {
            if !rest.is_empty() {
                return Err(OperatorCliError::invalid_input(
                    "check subcommand does not accept additional arguments",
                ));
            }
            ContextCommand::Check
        }
        "workflow" => {
            if rest.is_empty() {
                return Err(OperatorCliError::invalid_input(
                    "workflow command family requires subcommand arguments",
                ));
            }
            reject_protected_overrides(rest)?;
            ContextCommand::Forward {
                family: ChildFamily::Workflow,
                inner_args: rest.iter().map(|s| (*s).to_string()).collect(),
            }
        }
        "session" => {
            if rest.is_empty() {
                return Err(OperatorCliError::invalid_input(
                    "session command family requires subcommand arguments",
                ));
            }
            reject_protected_overrides(rest)?;
            ContextCommand::Forward {
                family: ChildFamily::Session,
                inner_args: rest.iter().map(|s| (*s).to_string()).collect(),
            }
        }
        "flow" => {
            if rest.is_empty() {
                return Err(OperatorCliError::invalid_input(
                    "flow command family requires subcommand arguments",
                ));
            }
            reject_protected_overrides(rest)?;
            ContextCommand::Forward {
                family: ChildFamily::Flow,
                inner_args: rest.iter().map(|s| (*s).to_string()).collect(),
            }
        }
        "help" => {
            return Err(OperatorCliError::invalid_input(
                "help command family is context-free and does not accept --context",
            ));
        }
        _ => {
            return Err(OperatorCliError::invalid_input(
                "unsupported command family; only check, workflow, session, and flow are supported",
            ));
        }
    };

    Ok(OuterParseAction::Execute {
        context_path: PathBuf::from(context_raw),
        command,
    })
}

fn is_protected_override_token(token: &str) -> bool {
    let trimmed = token.trim();
    PROTECTED_OVERRIDE_FLAGS.iter().any(|flag| {
        trimmed == *flag
            || trimmed
                .strip_prefix(*flag)
                .is_some_and(|suffix| suffix.starts_with('='))
    })
}

fn reject_protected_overrides(inner_args: &[&str]) -> Result<(), OperatorCliError> {
    for arg in inner_args {
        if is_protected_override_token(arg) {
            return Err(OperatorCliError::invalid_input(
                "protected scope or context flag overrides are not permitted",
            ));
        }
    }
    Ok(())
}

fn ensure_operator_platform_supported() -> Result<(), OperatorCliError> {
    ensure_platform_supported().map_err(|_| {
        OperatorCliError::new(
            "unsupported_platform",
            "trusted local operator CLI is only supported on Unix platforms",
        )
    })
}

fn run_sibling_help(family: ChildFamily) -> Result<ExitCode, OperatorCliError> {
    // Context-free family help never reads a context or contacts the supervisor. It resolves
    // only the canonical same-package sibling and forwards no user arguments.
    ensure_operator_platform_supported()?;
    let current_bin_dir = resolve_current_bin_dir()?;
    let sibling_exe = resolve_sibling_executable(&current_bin_dir, family)?;
    execute_sibling_command(&sibling_exe, &[OsString::from("--help")])
}

async fn run(context_path: &Path, command: ContextCommand) -> Result<ExitCode, OperatorCliError> {
    ensure_operator_platform_supported()?;

    let raw: RawPrivateContext = read_request(context_path)?;
    let current_bin_dir = resolve_current_bin_dir()?;
    let validated = validate_context(raw, &current_bin_dir)?;

    match command {
        ContextCommand::Check => {
            verify_binding(
                &validated.stores.state_dir,
                &validated.binding_id,
                &validated.thread_uuid,
            )
            .await?;

            let report = ContextCheckReport {
                schema_version: CONTEXT_SCHEMA_VERSION,
                project_root: validated.project_root,
                binding_id: validated.binding_id.clone(),
                thread_uuid: validated.thread_uuid,
                route: TRUSTED_LOCAL_OPERATOR_ROUTE,
                native_per_call_identity_verified: false,
                live_binding_verified: true,
                human_approval_verified: false,
                cli_package_pinned: validated.cli_package_pinned,
                stores: validated.stores,
            };
            write_result(&validated.binding_id, &report, false)?;
            Ok(ExitCode::SUCCESS)
        }
        ContextCommand::Forward { family, inner_args } => {
            let sibling_exe = resolve_sibling_executable(&validated.current_bin_dir, family)?;
            verify_binding(
                &validated.stores.state_dir,
                &validated.binding_id,
                &validated.thread_uuid,
            )
            .await?;

            let child_args = build_child_args(&validated, family, &inner_args);
            execute_sibling_command(&sibling_exe, &child_args)
        }
    }
}

fn validate_context(
    raw: RawPrivateContext,
    current_bin_dir: &Path,
) -> Result<ValidatedContext, OperatorCliError> {
    if raw.schema_version != CONTEXT_SCHEMA_VERSION {
        return Err(OperatorCliError::invalid_input(
            "context schema_version must be 1",
        ));
    }
    if raw.route != TRUSTED_LOCAL_OPERATOR_ROUTE {
        return Err(OperatorCliError::invalid_input(
            "context route must be trusted_local_operator",
        ));
    }
    if raw.native_per_call_identity_verified {
        return Err(OperatorCliError::invalid_input(
            "context native_per_call_identity_verified must be false",
        ));
    }

    let project_root = validate_existing_canonical_dir(&raw.project_root)?;

    let validated_dirs = validate_dirs(&[
        ("state_dir", &raw.stores.state_dir),
        ("registry_dir", &raw.stores.registry_dir),
        ("plans_dir", &raw.stores.plans_dir),
        ("authority_dir", &raw.stores.authority_dir),
        ("sessions_dir", &raw.stores.sessions_dir),
    ])?;

    if validated_dirs.iter().any(|dir| dir == &project_root) {
        return Err(OperatorCliError::new(
            "security_violation",
            "project_root must not match any private store directory",
        ));
    }

    let cli_package_pinned = match raw.cli_package {
        Some(ref cli_pkg_path) => {
            validate_cli_package_pin(cli_pkg_path, current_bin_dir)?;
            true
        }
        None => false,
    };

    Ok(ValidatedContext {
        project_root,
        binding_id: raw.binding_id,
        thread_uuid: raw.thread_uuid,
        stores: ValidatedStores {
            state_dir: validated_dirs[0].clone(),
            registry_dir: validated_dirs[1].clone(),
            plans_dir: validated_dirs[2].clone(),
            authority_dir: validated_dirs[3].clone(),
            sessions_dir: validated_dirs[4].clone(),
        },
        current_bin_dir: current_bin_dir.to_path_buf(),
        cli_package_pinned,
    })
}

fn resolve_current_bin_dir() -> Result<PathBuf, OperatorCliError> {
    let current_exe = std::env::current_exe().map_err(|_| {
        OperatorCliError::new(
            "io_error",
            "failed to determine current threadlane-context executable path",
        )
    })?;
    let canonical_exe = fs::canonicalize(&current_exe).map_err(|_| {
        OperatorCliError::new(
            "security_violation",
            "failed to canonicalize current threadlane-context executable path",
        )
    })?;
    let exe_meta = fs::symlink_metadata(&canonical_exe).map_err(|_| {
        OperatorCliError::new(
            "security_violation",
            "failed to inspect canonical threadlane-context executable metadata",
        )
    })?;
    if exe_meta.file_type().is_symlink() || !exe_meta.file_type().is_file() {
        return Err(OperatorCliError::new(
            "security_violation",
            "canonical threadlane-context executable must be a non-symlink regular file",
        ));
    }

    let parent = canonical_exe.parent().ok_or_else(|| {
        OperatorCliError::new(
            "security_violation",
            "canonical threadlane-context executable has no parent directory",
        )
    })?;
    validate_existing_canonical_dir(parent)
}

#[cfg(unix)]
fn validate_existing_canonical_dir(path: &Path) -> Result<PathBuf, OperatorCliError> {
    use std::os::unix::ffi::OsStrExt;

    let raw_bytes = path.as_os_str().as_bytes();
    if raw_bytes.is_empty() || !path.is_absolute() || raw_bytes.ends_with(b"/") {
        return Err(OperatorCliError::invalid_input(
            "directory path must be a normalized absolute directory path",
        ));
    }

    let mut normal_parts: Vec<&OsStr> = Vec::new();
    let mut saw_root = false;
    for component in path.components() {
        match component {
            Component::RootDir if !saw_root && normal_parts.is_empty() => {
                saw_root = true;
            }
            Component::Normal(part) if saw_root && !part.is_empty() => {
                normal_parts.push(part);
            }
            Component::CurDir | Component::ParentDir => {
                return Err(OperatorCliError::new(
                    "security_violation",
                    "directory path must not contain relative or traversal components",
                ));
            }
            _ => {
                return Err(OperatorCliError::invalid_input(
                    "directory path must be a normalized absolute directory path",
                ));
            }
        }
    }

    if !saw_root || normal_parts.is_empty() {
        return Err(OperatorCliError::invalid_input(
            "directory path must be a normalized absolute directory path",
        ));
    }

    let mut reconstructed = PathBuf::from("/");
    for part in &normal_parts {
        reconstructed.push(part);
    }
    if reconstructed.as_os_str() != path.as_os_str() {
        return Err(OperatorCliError::invalid_input(
            "directory path must be normalized without redundant separators or `.` segments",
        ));
    }

    let mut current = PathBuf::from("/");
    let root_meta = fs::symlink_metadata(&current).map_err(|_| {
        OperatorCliError::invalid_input("failed to inspect root directory metadata")
    })?;
    if root_meta.file_type().is_symlink() || !root_meta.file_type().is_dir() {
        return Err(OperatorCliError::new(
            "security_violation",
            "directory ancestor must be a non-symlink directory",
        ));
    }

    for part in &normal_parts {
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(OperatorCliError::new(
                        "security_violation",
                        "directory path must not contain symlinks",
                    ));
                }
                if !meta.file_type().is_dir() {
                    return Err(OperatorCliError::new(
                        "security_violation",
                        "directory path component is not a directory",
                    ));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(OperatorCliError::invalid_input(
                    "required directory does not exist",
                ));
            }
            Err(_) => {
                return Err(OperatorCliError::new(
                    "io_error",
                    "failed to inspect directory metadata",
                ));
            }
        }
    }

    let canonical = fs::canonicalize(&reconstructed).map_err(|_| {
        OperatorCliError::new(
            "security_violation",
            "failed to canonicalize directory path",
        )
    })?;
    if canonical != reconstructed {
        return Err(OperatorCliError::new(
            "security_violation",
            "directory path is not canonical",
        ));
    }

    Ok(canonical)
}

#[cfg(not(unix))]
fn validate_existing_canonical_dir(_path: &Path) -> Result<PathBuf, OperatorCliError> {
    Err(OperatorCliError::new(
        "unsupported_platform",
        "trusted local operator CLI is only supported on Unix platforms",
    ))
}

fn validate_cli_package_pin(
    cli_package: &Path,
    current_bin_dir: &Path,
) -> Result<(), OperatorCliError> {
    let canonical_pkg = validate_existing_canonical_dir(cli_package)?;
    let pkg_bin = canonical_pkg.join("bin");
    if pkg_bin == current_bin_dir {
        Ok(())
    } else {
        Err(OperatorCliError::new(
            "security_violation",
            "context cli_package does not match current executable package bin directory",
        ))
    }
}

fn resolve_sibling_executable(
    current_bin_dir: &Path,
    family: ChildFamily,
) -> Result<PathBuf, OperatorCliError> {
    let clean_bin_dir = validate_existing_canonical_dir(current_bin_dir)?;
    let candidate = clean_bin_dir.join(family.binary_name());

    let meta = match fs::symlink_metadata(&candidate) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(OperatorCliError::new(
                "not_configured",
                "required sibling operator executable does not exist",
            ));
        }
        Err(_) => {
            return Err(OperatorCliError::new(
                "io_error",
                "failed to inspect sibling operator executable metadata",
            ));
        }
    };

    if meta.file_type().is_symlink() {
        return Err(OperatorCliError::new(
            "security_violation",
            "sibling operator executable must not be a symlink",
        ));
    }
    if !meta.file_type().is_file() {
        return Err(OperatorCliError::new(
            "security_violation",
            "sibling operator executable must be a regular file",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if (meta.permissions().mode() & 0o111) == 0 {
            return Err(OperatorCliError::new(
                "security_violation",
                "sibling operator executable is not marked executable",
            ));
        }
    }

    let canonical = fs::canonicalize(&candidate).map_err(|_| {
        OperatorCliError::new(
            "security_violation",
            "failed to canonicalize sibling operator executable",
        )
    })?;
    if canonical != candidate || canonical.parent() != Some(clean_bin_dir.as_path()) {
        return Err(OperatorCliError::new(
            "security_violation",
            "sibling operator executable is not canonical within current bin directory",
        ));
    }

    Ok(canonical)
}

fn build_child_args(
    validated: &ValidatedContext,
    family: ChildFamily,
    inner_args: &[String],
) -> Vec<OsString> {
    let mut args = Vec::with_capacity(14 + inner_args.len());
    args.push(OsString::from("--state-dir"));
    args.push(validated.stores.state_dir.as_os_str().to_os_string());
    args.push(OsString::from("--registry-dir"));
    args.push(validated.stores.registry_dir.as_os_str().to_os_string());
    args.push(OsString::from("--plans-dir"));
    args.push(validated.stores.plans_dir.as_os_str().to_os_string());

    if matches!(family, ChildFamily::Session | ChildFamily::Flow) {
        args.push(OsString::from("--authority-dir"));
        args.push(validated.stores.authority_dir.as_os_str().to_os_string());
        args.push(OsString::from("--sessions-dir"));
        args.push(validated.stores.sessions_dir.as_os_str().to_os_string());
    }

    args.push(OsString::from("--binding"));
    args.push(OsString::from(&validated.binding_id));
    args.push(OsString::from("--thread"));
    args.push(OsString::from(&validated.thread_uuid));

    for arg in inner_args {
        args.push(OsString::from(arg));
    }
    args
}

fn execute_sibling_command(
    sibling_exe: &Path,
    child_args: &[OsString],
) -> Result<ExitCode, OperatorCliError> {
    let mut child = Command::new(sibling_exe)
        .args(child_args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| match err.kind() {
            io::ErrorKind::NotFound => OperatorCliError::new(
                "not_configured",
                "required sibling operator executable does not exist",
            ),
            io::ErrorKind::PermissionDenied => OperatorCliError::new(
                "security_violation",
                "permission denied when spawning sibling operator executable",
            ),
            _ => OperatorCliError::new(
                "io_error",
                "failed to spawn sibling operator executable before execution",
            ),
        })?;

    let status = child.wait().map_err(|_| {
        OperatorCliError::new(
            "outcome_unknown",
            "sibling operator command was spawned, but waiting for its completion failed; reconcile state before retrying",
        )
    })?;

    let Some(code) = status.code() else {
        return Err(OperatorCliError::new(
            "outcome_unknown",
            "sibling operator command terminated by signal after spawn; reconcile state before retrying",
        ));
    };

    if code == 0 {
        Ok(ExitCode::SUCCESS)
    } else {
        let exit_byte = u8::try_from(code).ok().filter(|&b| b != 0).unwrap_or(1);
        Ok(ExitCode::from(exit_byte))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::ffi::OsStringExt;

    use super::*;

    #[test]
    fn outer_parser_handles_help_version_and_rejects_invalid_or_protected_args() {
        let help = parse_outer_args(&[OsString::from("--help")]).expect("parse --help");
        assert_eq!(help, OuterParseAction::DisplayHelp);

        let short_help = parse_outer_args(&[OsString::from("-h")]).expect("parse -h");
        assert_eq!(short_help, OuterParseAction::DisplayHelp);

        let ver = parse_outer_args(&[OsString::from("--version")]).expect("parse --version");
        assert_eq!(ver, OuterParseAction::DisplayVersion);

        let check = parse_outer_args(&[
            OsString::from("--context"),
            OsString::from("/tmp/context.json"),
            OsString::from("check"),
        ])
        .expect("parse check");
        assert_eq!(
            check,
            OuterParseAction::Execute {
                context_path: PathBuf::from("/tmp/context.json"),
                command: ContextCommand::Check,
            }
        );

        let eq_workflow = parse_outer_args(&[
            OsString::from("--context=/tmp/context.json"),
            OsString::from("workflow"),
            OsString::from("catalog"),
            OsString::from("--limit"),
            OsString::from("5"),
        ])
        .expect("parse --context= workflow");
        assert_eq!(
            eq_workflow,
            OuterParseAction::Execute {
                context_path: PathBuf::from("/tmp/context.json"),
                command: ContextCommand::Forward {
                    family: ChildFamily::Workflow,
                    inner_args: vec![
                        "catalog".to_string(),
                        "--limit".to_string(),
                        "5".to_string()
                    ],
                },
            }
        );

        let flow = parse_outer_args(&[
            OsString::from("--context"),
            OsString::from("/tmp/context.json"),
            OsString::from("flow"),
            OsString::from("fixture-command"),
            OsString::from("--limit"),
            OsString::from("5"),
        ])
        .expect("parse flow");
        assert_eq!(
            flow,
            OuterParseAction::Execute {
                context_path: PathBuf::from("/tmp/context.json"),
                command: ContextCommand::Forward {
                    family: ChildFamily::Flow,
                    inner_args: vec![
                        "fixture-command".to_string(),
                        "--limit".to_string(),
                        "5".to_string()
                    ],
                },
            }
        );

        // Non-UTF8 is rejected safely with invalid_input.
        let bad_utf8 = parse_outer_args(&[
            OsString::from("--context"),
            OsString::from_vec(vec![0xff, 0xfe]),
            OsString::from("check"),
        ])
        .expect_err("non-utf8 must fail");
        assert_eq!(bad_utf8.code, "invalid_input");

        // Unsupported families (approve, host, operator) are rejected.
        for unsupported in ["approve", "host", "operator"] {
            let err = parse_outer_args(&[
                OsString::from("--context"),
                OsString::from("/tmp/context.json"),
                OsString::from(unsupported),
                OsString::from("prepare"),
            ])
            .expect_err("unsupported family must fail");
            assert_eq!(err.code, "invalid_input");
        }

        // Protected flags in inner_args (separate and equals forms) are rejected for every
        // forwarding family, including flow.
        for (family, inner) in [
            ("workflow", "catalog"),
            ("session", "session"),
            ("flow", "fixture-command"),
        ] {
            for bad_flag in [
                "--state-dir",
                "--state-dir=/tmp/other",
                "--registry-dir",
                "--registry-dir=/tmp/other",
                "--plans-dir",
                "--plans-dir=/tmp/other",
                "--authority-dir",
                "--authority-dir=/tmp/other",
                "--sessions-dir",
                "--sessions-dir=/tmp/other",
                "--binding",
                "--binding=bnd_other",
                "--thread",
                "--thread=0196ed00-0000-7000-8000-000000000099",
                "--context",
                "--context=/tmp/other.json",
                "--project-root=/tmp/other",
            ] {
                let err = parse_outer_args(&[
                    OsString::from("--context"),
                    OsString::from("/tmp/context.json"),
                    OsString::from(family),
                    OsString::from(inner),
                    OsString::from(bad_flag),
                ])
                .expect_err("protected flag must fail");
                assert_eq!(err.code, "invalid_input");
            }
        }

        // A protected flag cannot occupy the family position, and repeated protected flags are
        // rejected before any child could be resolved.
        for raw in [
            vec![
                OsString::from("--context"),
                OsString::from("/tmp/context.json"),
                OsString::from("--state-dir=/tmp/other"),
                OsString::from("flow"),
                OsString::from("fixture-command"),
            ],
            vec![
                OsString::from("--context"),
                OsString::from("/tmp/context.json"),
                OsString::from("flow"),
                OsString::from("fixture-command"),
                OsString::from("--thread"),
                OsString::from("0196ed00-0000-7000-8000-000000000099"),
                OsString::from("--thread"),
                OsString::from("0196ed00-0000-7000-8000-000000000099"),
            ],
        ] {
            let err = parse_outer_args(&raw).expect_err("protected family/repeat must fail");
            assert_eq!(err.code, "invalid_input");
        }
    }

    #[test]
    fn outer_parser_accepts_only_exact_context_free_family_help_forms() {
        let bare_help = parse_outer_args(&[OsString::from("help")]).expect("parse bare help");
        assert_eq!(bare_help, OuterParseAction::DisplayHelp);

        for (family_token, expected_family) in [
            ("workflow", ChildFamily::Workflow),
            ("session", ChildFamily::Session),
            ("flow", ChildFamily::Flow),
        ] {
            let parsed = parse_outer_args(&[OsString::from("help"), OsString::from(family_token)])
                .expect("parse context-free family help");
            assert_eq!(
                parsed,
                OuterParseAction::DisplaySiblingHelp {
                    family: expected_family,
                }
            );
        }

        let invalid_help_forms: &[&[&str]] = &[
            &["help", "check"],
            &["help", "approve"],
            &["help", "host"],
            &["help", "operator"],
            &["help", "unknown"],
            &["help", "workflow=ignored"],
            &["help", "flow=ignored"],
            &["help", "--help"],
            &["help", "-h"],
            &["help", "--context"],
            &["help", "--context=/tmp/context.json"],
            &["help", "workflow", "extra"],
            &["help", "session", "--help"],
            &["help", "flow", "extra"],
            &["help", "flow", "--help"],
            &["help", "workflow", "--state-dir=/tmp/other"],
            &["help", "flow", "--state-dir=/tmp/other"],
        ];
        for args in invalid_help_forms {
            let raw: Vec<OsString> = args.iter().map(|arg| OsString::from(*arg)).collect();
            let err = parse_outer_args(&raw).expect_err("non-exact help form must fail");
            assert_eq!(err.code, "invalid_input");
        }

        let context_bearing_help_forms: &[&[&str]] = &[
            &["--context", "/tmp/context.json", "help", "workflow"],
            &["--context=/tmp/context.json", "help", "session"],
            &["--context", "/tmp/context.json", "help", "flow"],
            &["--context", "/tmp/context.json", "help"],
        ];
        for args in context_bearing_help_forms {
            let raw: Vec<OsString> = args.iter().map(|arg| OsString::from(*arg)).collect();
            let err = parse_outer_args(&raw).expect_err("context-bearing help must fail");
            assert_eq!(err.code, "invalid_input");
        }
    }
}
