{
  description = "magic-cap is a command line utility for an always encrypted archive file type.";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.11";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = { url = "github:oxalica/rust-overlay"; };
  };
  outputs = { nixpkgs, nixpkgs-unstable, rust-overlay, ... }:
    let
      system = "x86_64-linux";
    in {
      packages.${system}.default =
        let
          pkgs = import nixpkgs { inherit system; };
            in pkgs.rustPlatform.buildRustPackage {
              pname = "iroh_lan";
              buildInputs = [ ];
              version = "0.1.0";
              cargoLock.lockFile = ./Cargo.lock;
              src = pkgs.lib.cleanSource ./.;
            };
            devShells.${system}.default =
              let pkgs = import nixpkgs {
                    inherit system;
                    overlays = [ (import rust-overlay) ];
                    config.allowUnfree = true;
                  };
                  upkgs = import nixpkgs-unstable { inherit system; };
              in
                pkgs.mkShell {
                  packages = with pkgs; [
                    alsa-lib
                    autoconf
                    automake
                    dbus
                    egl-wayland
                    libGL
                    libtool
                    libxkbcommon
                    pkg-config
                    pkgs.rustPlatform.bindgenHook
                    rust-bin.nightly.latest.default
                    upkgs.rust-analyzer
                    wayland
                    wayland.dev
                    # rust-bin.stable.latest.default
                    # upkgs.rust-analyzer
                    # cargo
                    # pkg-config
                    # clippy
                    # vala
                    # cairo
                    # atkmm
                    # gdk-pixbuf
                    # gdk-pixbuf-xlib
                    # gnome2.gtkglext
                    # pango
                    # libsoup_3
                    # webp-pixbuf-loader
                    # webkitgtk_4_1
                    # pnpm
                    # nodejs
                    # gnumake
                    # docker
                    # docker-compose
                  ];
                };
    };
}
