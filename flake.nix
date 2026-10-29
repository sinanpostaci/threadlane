{
  description = "Threadlane development shells and source packages";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;

      packagesFor = system:
        import ./nix/packages.nix {
          pkgs = import nixpkgs { inherit system; };
          src = self;
        };
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              rustc
              cargo
              rustfmt
              clippy
              pkg-config
              gcc
              git
              python3
            ];
          };

          desktop = pkgs.mkShell {
            packages = (with pkgs; [
              rustc
              cargo
              rustfmt
              clippy
              pkg-config
              gcc
              git
              python3
              cargo-tauri
              nodejs
            ]) ++ pkgs.lib.optionals pkgs.stdenv.isLinux (with pkgs; [
              gtk3
              webkitgtk_4_1
              libsoup_3
              openssl
              librsvg
              glib-networking
              gsettings-desktop-schemas
            ]) ++ pkgs.lib.optionals pkgs.stdenv.isDarwin (with pkgs; [
              openssl
              libiconv
            ]);

            shellHook = pkgs.lib.optionalString pkgs.stdenv.isLinux ''
              export GIO_EXTRA_MODULES="${pkgs.glib-networking}/lib/gio/modules''${GIO_EXTRA_MODULES:+:$GIO_EXTRA_MODULES}"
              export XDG_DATA_DIRS="${pkgs.gsettings-desktop-schemas}/share/gsettings-schemas/${pkgs.gsettings-desktop-schemas.name}:${pkgs.gtk3}/share/gsettings-schemas/${pkgs.gtk3.name}:${pkgs.gtk3}/share:''${XDG_DATA_DIRS:-/usr/local/share:/usr/share}"
            '';
          };
        });

      packages = forAllSystems packagesFor;

      apps = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          packages = packagesFor system;
          threadlaneApp = {
            type = "app";
            program = "${packages.threadlane}/bin/threadlane";
          };
        in
        {
          default = threadlaneApp;
          threadlane = threadlaneApp;
        }
        // nixpkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          "threadlane-desktop" = {
            type = "app";
            program = "${packages."threadlane-desktop"}/bin/threadlane-desktop";
          };
        });
    };
}
