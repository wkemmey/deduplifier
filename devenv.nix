{ pkgs, lib, ... }:

{
  # rust development environment with common tools and configurations
  
  languages.rust = {
    enable = true;
    channel = "stable";
    targets = [ "x86_64-pc-windows-gnu" ];
  };

  # common rust development packages
  packages = with pkgs; [
    # cross-compilation to Windows: expose the mingw-w64 gcc on PATH without
    # setting CC, so the native Linux CC is not overridden.
    pkgsCross.mingwW64.buildPackages.gcc

    # build tools
    cargo-watch      # watch for changes and rebuild
    cargo-edit       # cargo add, cargo rm, cargo upgrade commands
    cargo-outdated   # check for outdated dependencies
    cargo-audit      # security vulnerability scanner
    cargo-bloat      # find what takes most space in binary
    cargo-expand     # expand macros
    cargo-flamegraph # profiling tool
    
    # testing and benchmarking
    cargo-nextest    # better test runner
    cargo-tarpaulin  # code coverage
    
    # documentation
    cargo-readme     # generate README from doc comments
    
    # database tools
    sqlite           # sqlite command line interface
    sqlitebrowser    # gui for sqlite databases
    
    # additional tools
    bacon            # background code checker
    rust-analyzer    # LSP server (devenv includes this but explicit is good)
  ];

  # git hooks for code quality
  git-hooks.hooks = {
    clippy.enable = true;   # linter
  };

  # environment variables
  env = {
    RUST_BACKTRACE = "1";
    # The mingw gcc package in `packages` clobbers CC with the cross-compiler.
    # Override it back to the native gcc so `cargo build/test` (without a target)
    # compiles for Linux. CC_x86_64_pc_windows_gnu tells build scripts (e.g.
    # libsqlite3-sys) which C compiler to use when cross-compiling C for Windows.
    CC = "gcc";
    CXX = "g++";
    CC_x86_64_pc_windows_gnu = "x86_64-w64-mingw32-gcc";
  };

  # Restore CC in interactive devenv shells too (enterShell runs on `devenv shell`).
  enterShell = ''
    export CC=gcc
    export CXX=g++
  '';

  # scripts for common tasks
  scripts = {
    # run with watching
    dev.exec = "cargo watch -x run";
    
    # run tests with watching
    test-watch.exec = "cargo watch -x test";
    
    # run with backtrace
    run-debug.exec = "RUST_BACKTRACE=full cargo run";
    
    # build a Windows .exe
    build-windows.exec = ''cargo build --release --target x86_64-pc-windows-gnu'';

    build-windows-test.exec = ''cargo test --no-run --target x86_64-pc-windows-gnu'';

    # check everything
    check-all.exec = ''
      cargo fmt --check &&
      cargo clippy -- -D warnings &&
      cargo test &&
      cargo audit
    '';
  };
}
