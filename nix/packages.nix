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
  desktopNpmDepsHash = "sha256-X5rZ5gmquTWSPoHlMrAXKmJ72RDB+hki/RRWrrpLb9Q=";

  desktopLibs = with pkgs; [
    gtk3
    webkitgtk_4_1
    libsoup_3
    openssl
    librsvg
    glib-networking
    gsettings-desktop-schemas
    libxkbcommon
    xorg.libX11
    xorg.libXcursor
    xorg.libXrandr
    xorg.libXi
    xorg.libXext
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

in
{
  inherit threadlane;
  default = threadlane;
}
// lib.optionalAttrs pkgs.stdenv.isLinux {
  "threadlane-desktop" = threadlaneDesktop;
}
