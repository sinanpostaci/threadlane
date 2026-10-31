{
  pkgs,
  chatgpt,
}:

assert pkgs.stdenv.hostPlatform.isLinux;

let
  pythonEnv = pkgs.python3.withPackages (ps: [
    ps.websockets
  ]);

  launcherScript = builtins.path {
    path = ./launcher.py;
    name = "threadlane-codex-launcher.py";
  };

  adapted = chatgpt.unwrapped.overrideAttrs (oldAttrs: {
    nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ]) ++ [
      pkgs.makeWrapper
    ];

    postInstall = (oldAttrs.postInstall or "") + ''
      codexBin=""
      if [ -e "$out/resources/codex" ]; then
        codexBin="$out/resources/codex"
      else
        for candidate in "$out"/*/resources/codex "$out"/*/*/resources/codex; do
          if [ -e "$candidate" ]; then
            if [ -n "$codexBin" ]; then
              echo "Multiple resources/codex paths found under $out" >&2
              exit 1
            fi
            codexBin="$candidate"
          fi
        done
      fi

      if [ -z "$codexBin" ]; then
        echo "Bundled resources/codex not found under $out" >&2
        exit 1
      fi

      codexDir="$(dirname "$codexBin")"
      realCodex="$codexDir/codex-threadlane-original"

      mv "$codexBin" "$realCodex"
      makeWrapper "${pythonEnv}/bin/python3" "$codexBin" \
        --add-flags "${launcherScript}" \
        --set THREADLANE_REAL_CODEX "$realCodex"
    '';
  });
in
chatgpt.override {
  chatgpt-unwrapped = adapted;
}
