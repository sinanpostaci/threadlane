{ pkgs }:

let
  webkitgtk = pkgs.webkitgtk_4_1;
  upstreamFixCommit = "bfae1a7b27ec99a467d0669299465491ecfa4611";

  # Hash only the patch file so unrelated repository edits do not invalidate
  # the compiled WebKitGTK derivation in flake builds.
  damageClearPatch = builtins.path {
    path = ./patches/webkitgtk-noncomposited-damage-clear.patch;
    name = "webkitgtk-noncomposited-damage-clear.patch";
  };
in
if (webkitgtk.version or null) == "2.54.1" then
  webkitgtk.overrideAttrs (prevAttrs: {
    patches = (prevAttrs.patches or [ ]) ++ [
      damageClearPatch
    ];

    passthru = (prevAttrs.passthru or { }) // {
      nonCompositedDamageClearFixCommit = upstreamFixCommit;
      threadlaneBackports = {
        nonCompositedDamageClear = {
          upstreamCommit = upstreamFixCommit;
          upstreamCanonical = "https://commits.webkit.org/324966@main";
          upstreamUrl = "https://github.com/WebKit/WebKit/commit/${upstreamFixCommit}";
          upstreamBug = "https://bugs.webkit.org/show_bug.cgi?id=324543";
          targetVersion = "2.54.1";
          patch = damageClearPatch;
        };
      };
    };
  })
else
  webkitgtk
