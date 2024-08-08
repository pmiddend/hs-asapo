# SPDX-FileCopyrightText: 2021 Serokell <https://serokell.io/>
#
# SPDX-License-Identifier: CC0-1.0

{
  description = "hs-asapo";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-24.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ ];
          };

          haskellPackages = pkgs.haskellPackages.override
            {
              overrides = self: super: { };
            };

          packageName = "hs-asapo";

          asapo-core = with pkgs; stdenv.mkDerivation rec {
            pname = "asapo";

            version = "23.11.1";

            src = fetchurl {
              url = "https://gitlab.desy.de/asapo/asapo/-/archive/${version}/asapo-${version}.tar.gz";
              hash = "sha256-eEKYEjGkUIIsOEjKDWg+4SnqtHTMSDak6lnyxnWI1FU=";
            };

            nativeBuildInputs = [ cmake ];

            buildInputs = [
              curl
              rdkafka
              mongoc
              cyrus_sasl
              python3
              python3Packages.cython
              python3Packages.numpy
            ];

            cmakeFlags = [
              "-DBUILD_PYTHON=OFF"
              # This is actually just to let cmake not build the clients. We
              # build them ourselves, with Nix methods.
              "-DBUILD_CLIENTS_ONLY=ON"
            ];

            # Currently, asapo needs git to evaluate the current branch, which
            # doesn't work when you have a tar file as the source.
            # patches = [ ./remove-git-references.patch ./fix-kDefaultIngestMode.patch ];
            patches = [ ./remove-git-references.patch ];
          };
        in
        {
          packages.${packageName} =
            haskellPackages.callCabal2nix packageName self { libasapo-consumer = asapo-core; libasapo-producer = asapo-core; };

          packages.default = self.packages.${system}.${packageName};

          packages.asapo-core = asapo-core;

          defaultPackage = self.packages.${system}.default;

          devShells.default =
            pkgs.mkShell {
              buildInputs = with pkgs; [
                haskellPackages.haskell-language-server
                cabal-install
                ghcid
                haskellPackages.hlint
                haskellPackages.apply-refact
                asapo-core
                pkg-config
              ];
              inputsFrom = [ self.packages.${system}.hs-asapo.env ];
            };
          devShell = self.devShells.${system}.default;
        });
}
