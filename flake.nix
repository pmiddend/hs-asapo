# SPDX-FileCopyrightText: 2021 Serokell <https://serokell.io/>
#
# SPDX-License-Identifier: CC0-1.0

{
  description = "hs-asapo";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-26.05";
    flake-utils.url = "github:numtide/flake-utils";
    desy-flake = {
      url = "git+https://gitlab.desy.de/philipp.middendorf/desy-flake";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, desy-flake }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ desy-flake.overlays.default ];
          };

          haskellPackages = pkgs.haskellPackages.override
            {
              overrides = self: super: { };
            };

          packageName = "hs-asapo";
        in
        {
          packages.${packageName} =
            haskellPackages.callCabal2nix packageName self {
              libasapo-consumer = pkgs.asapo-libs;
              libasapo-producer = pkgs.asapo-libs;
            };

          packages.default = self.packages.${system}.${packageName};

          packages.asapo-libs = pkgs.asapo-libs;

          defaultPackage = self.packages.${system}.default;

          devShells.default =
            pkgs.mkShell {
              buildInputs = with pkgs; [
                haskellPackages.haskell-language-server
                cabal-install
                gdb
                ghcid
                haskellPackages.hlint
                asapo-libs
                pkg-config
              ];
              inputsFrom = [ self.packages.${system}.hs-asapo.env ];

              # From https://github.com/ulidtko/cabal-doctest
              # These environment variables are important. Without these,
              # doctest doesn't pick up nix's version of ghc, and will fail
              # claiming it can't find your dependencies
              shellHook =
                let
                  myHaskell = (pkgs.haskellPackages.ghcWithHoogle (p: with p; [
                    clock
                    timerep
                    text
                    time
                    bytestring
                    doctest
                  ]));
                in
                ''
                  export NIX_GHC=${myHaskell}/bin/ghc
                  export NIX_GHC_LIBDIR=${myHaskell}/lib/ghc-${haskellPackages.ghc.version}/lib
                '';
            };
          devShell = self.devShells.${system}.default;
        });
}
