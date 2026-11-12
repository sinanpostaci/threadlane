{ pkgs, src }:

let
  lib = pkgs.lib;
  version = "0.1.0";

  source = import ./source.nix { inherit lib src; };

  # Embed only the helper text in the runtime closure. The source checkout is
  # not part of the packaged desktop environment.
  desktopEnvironment = pkgs.writeText "threadlane-desktop-environment.sh" (
    builtins.readFile ../scripts/desktop-environment.sh
  );

  # Recompute only when apps/desktop/package-lock.json changes.
  desktopNpmDepsHash = "sha256-jtrBh3znCB27fwtgbv0AYW67itZ59ZiqOqomfzKzNiY=";

  desktopWebkitgtk = import ./webkitgtk.nix { inherit pkgs; };

  desktopLibs = with pkgs; [
    gtk3
    gtk-layer-shell
    desktopWebkitgtk
    libsoup_3
    openssl
    librsvg
    glib-networking
    gsettings-desktop-schemas
    libxkbcommon
    # Legacy attributes are evaluated only when the current name is unavailable.
    (pkgs.libx11 or pkgs.xorg.libX11)
    (pkgs.libxcursor or pkgs.xorg.libXcursor)
    (pkgs.libxrandr or pkgs.xorg.libXrandr)
    (pkgs.libxi or pkgs.xorg.libXi)
    (pkgs.libxext or pkgs.xorg.libXext)
  ];

  desktopFrontend = pkgs.buildNpmPackage {
    pname = "threadlane-desktop-frontend";
    inherit version;
    src = source + "/apps/desktop";
    npmDepsHash = desktopNpmDepsHash;
    npmBuildScript = "build";

    # The package only supplies the embedded web assets for the Tauri binary;
    # no Node install tree is needed in the final Nix output.
    installPhase = ''
      runHook preInstall
      mkdir -p "$out"
      cp -r dist "$out/dist"
      runHook postInstall
    '';

    meta = {
      description = "Compiled Vite frontend assets for the Threadlane desktop panel";
      license = with lib.licenses; [ mit asl20 ];
    };
  };

  desktopNpmModules = pkgs.buildNpmPackage {
    pname = "threadlane-desktop-node-modules";
    inherit version;
    src = source + "/apps/desktop";
    npmDepsHash = desktopNpmDepsHash;
    dontNpmBuild = true;

    installPhase = ''
      runHook preInstall
      mkdir -p "$out/lib"
      cp -r node_modules "$out/lib/node_modules"
      runHook postInstall
    '';

    meta = {
      description = "Locked npm dependency tree for Threadlane desktop browser acceptance";
      license = with lib.licenses; [ mit asl20 ];
    };
  };

  # Both Cargo builds are driven by committed lockfiles. The parent owns
  # Cargo.lock changes after workspace integration; no fixed-output Cargo hash
  # is fabricated here.
  threadlane = pkgs.rustPlatform.buildRustPackage {
    pname = "threadlane";
    inherit version;
    src = source;
    cargoLock.lockFile = source + "/Cargo.lock";
    cargoBuildFlags = [
      "--locked"
      "--package" "threadlane"
      "--package" "threadlane-native"
      "--package" "threadlane-host"
      "--package" "threadlane-operations"
      "--package" "threadlane-decisions"
      "--package" "threadlane-codex-bridge"
    ];
    nativeBuildInputs = [ pkgs.pkg-config ];
    doCheck = false;

    meta = {
      description = "Threadlane local supervisor, trusted CLI, and stdio MCP adapter";
      license = with lib.licenses; [ mit asl20 ];
      mainProgram = "threadlane";
      platforms = lib.platforms.unix;
    };
  };

  threadlaneDesktop = pkgs.rustPlatform.buildRustPackage {
    pname = "threadlane-desktop";
    inherit version;
    src = source;
    cargoRoot = "apps/desktop/src-tauri";
    buildAndTestSubdir = "apps/desktop/src-tauri";
    cargoLock.lockFile = source + "/apps/desktop/src-tauri/Cargo.lock";
    # Tauri treats builds without this dependency feature as development builds
    # and tries to load tauri.conf.json's devUrl instead of the embedded assets.
    cargoBuildFlags = [
      "--locked"
      "--features" "tauri/custom-protocol"
    ];
    doCheck = false;
    passthru.frontend = desktopFrontend;

    nativeBuildInputs = [
      pkgs.pkg-config
      pkgs.makeWrapper
    ];
    buildInputs = desktopLibs;

    # cargo build reads the embedded frontend from apps/desktop/dist. Build the
    # frontend as its own locked-npm derivation instead of invoking a Tauri CLI
    # or relying on the developer's node_modules directory.
    postPatch = ''
      mkdir -p apps/desktop/dist
      cp -r ${desktopFrontend}/dist/. apps/desktop/dist/
    '';

    postFixup = ''
      wrapProgram "$out/bin/threadlane-desktop" \
        --prefix PATH : "${lib.makeBinPath [ threadlane ]}" \
        --prefix LD_LIBRARY_PATH : "${lib.makeLibraryPath desktopLibs}" \
        --prefix GIO_EXTRA_MODULES : "${pkgs.glib-networking}/lib/gio/modules" \
        --prefix XDG_DATA_DIRS : "${pkgs.gsettings-desktop-schemas}/share/gsettings-schemas/${pkgs.gsettings-desktop-schemas.name}:${pkgs.gtk3}/share/gsettings-schemas/${pkgs.gtk3.name}:${pkgs.gtk3}/share" \
        --run "source ${desktopEnvironment}; threadlane_configure_desktop_environment"
    '';

    meta = {
      description = "Threadlane Tauri 2 desktop control panel";
      license = with lib.licenses; [ mit asl20 ];
      mainProgram = "threadlane-desktop";
      platforms = lib.platforms.linux;
    };
  };

  desktopPlaywright = pkgs.writeShellApplication {
    name = "threadlane-desktop-playwright";
    runtimeInputs = [
      pkgs.coreutils
      pkgs.findutils
      pkgs.git
      pkgs.nodejs
      pkgs.playwright-test
      pkgs.python3
    ];
    text = ''
      desktop_src="''${THREADLANE_DESKTOP_SOURCE_DIR:-${source}/apps/desktop}"
      export THREADLANE_DESKTOP_SOURCE_DIR="$desktop_src"

      stage_dir="$(python3 "${source}/scripts/desktop-acceptance.py" --prepare-playwright-stage)"

      for item in index.html package.json tsconfig.json vite.config.ts playwright.config.ts src tests public; do
        if [ -e "$desktop_src/$item" ]; then
          cp -rL "$desktop_src/$item" "$stage_dir/$item"
          chmod -R u+w "$stage_dir/$item"
        fi
      done

      required_icon="src-tauri/icons/icon.svg"
      if [ ! -f "$desktop_src/$required_icon" ]; then
        echo "error: required desktop icon asset missing: $desktop_src/$required_icon" >&2
        exit 1
      fi
      mkdir -p "$stage_dir/src-tauri/icons"
      cp -L "$desktop_src/$required_icon" "$stage_dir/$required_icon"
      chmod -R u+w "$stage_dir/src-tauri"

      for entry in "${desktopNpmModules}/lib/node_modules"/* "${desktopNpmModules}/lib/node_modules"/.[!.]*; do
        if [ ! -e "$entry" ]; then
          continue
        fi
        name="$(basename "$entry")"
        case "$name" in
          .vite)
            continue
            ;;
          @*)
            mkdir -p "$stage_dir/node_modules/$name"
            for scoped in "$entry"/*; do
              if [ -e "$scoped" ]; then
                ln -sfn "$scoped" "$stage_dir/node_modules/$name/$(basename "$scoped")"
              fi
            done
            ;;
          *)
            ln -sfn "$entry" "$stage_dir/node_modules/$name"
            ;;
        esac
      done

      link_pw_pkg() {
        local pkg_rel="$1"
        local direct="${pkgs.playwright-test}/lib/node_modules/$pkg_rel"
        local found=""
        if [ -d "$direct" ]; then
          found="$direct"
        else
          found="$(find -L "${pkgs.playwright-test}/lib/node_modules" -maxdepth 5 -type d -path "*/node_modules/$pkg_rel" -print -quit)"
        fi
        if [ -z "$found" ]; then
          echo "error: unable to locate $pkg_rel inside ${pkgs.playwright-test}" >&2
          exit 1
        fi
        mkdir -p "$(dirname "$stage_dir/node_modules/$pkg_rel")"
        ln -sfn "$found" "$stage_dir/node_modules/$pkg_rel"
      }

      link_pw_pkg "@playwright/test"
      link_pw_pkg "playwright"
      link_pw_pkg "playwright-core"

      export PLAYWRIGHT_BROWSERS_PATH="${pkgs.playwright-driver.browsers}"
      export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD="1"
      export PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS="true"

      cd "$stage_dir"
      exec "${pkgs.playwright-test}/bin/playwright" test --config playwright.config.ts "$@"
    '';
  };

  desktopAcceptanceVm = import ./desktop-acceptance.nix {
    inherit
      pkgs
      lib
      threadlane
      threadlaneDesktop
      ;
  };

in
{
  inherit threadlane;
  default = threadlane;
}
// lib.optionalAttrs pkgs.stdenv.isLinux {
  "threadlane-desktop" = threadlaneDesktop;
  "threadlane-desktop-playwright" = desktopPlaywright;
  "desktop-acceptance-vm" = desktopAcceptanceVm;
}
