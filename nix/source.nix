{ lib, src }:

# Keep package sources reproducible and free of local runtime/build data. A
# filter on directory names stops the walker before it copies large ignored
# trees such as target/, node_modules/, .local/ and .cache/.
lib.cleanSourceWith {
  inherit src;
  name = "threadlane-source";
  filter = path: _type:
    let
      base = baseNameOf path;
      ignoredNames = [
        ".git"
        ".direnv"
        ".local"
        ".cache"
        "target"
        "node_modules"
        "dist"
        "__pycache__"
        ".pytest_cache"
        ".mypy_cache"
        ".ruff_cache"
        "result"
      ];
      ignoredRuntimeFile =
        base == ".env"
        || lib.hasPrefix ".env." base
        || lib.hasSuffix ".pyc" base
        || base == "state.db"
        || lib.hasPrefix "state.db-" base
        || lib.hasSuffix ".db" base
        || lib.hasSuffix ".db-shm" base
        || lib.hasSuffix ".db-wal" base
        || lib.hasSuffix ".sqlite" base
        || lib.hasSuffix ".sqlite-shm" base
        || lib.hasSuffix ".sqlite-wal" base
        || base == "supervisor.json"
        || base == "supervisor.lock"
        || lib.hasPrefix ".supervisor.json.tmp." base;
    in
    !(builtins.elem base ignoredNames) && !ignoredRuntimeFile;
}
