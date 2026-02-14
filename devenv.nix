{ pkgs, lib, ... }:

{
  # rust development environment with common tools and configurations
  
  languages.rust = {
    enable = true;
    # using stable channel by default, can override per-project
    # channel = "stable"; # requires rust-overlay input
  };

  # common rust development packages
  packages = with pkgs; [
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
    
    # additional tools
    bacon            # background code checker
    rust-analyzer    # LSP server (devenv includes this but explicit is good)
  ];

  # git hooks for code quality
  git-hooks.hooks = {
    rustfmt.enable = true;  # auto-format code
    clippy.enable = true;   # linter
  };

  # environment variables
  env = {
    RUST_BACKTRACE = "1";
  };

  # scripts for common tasks
  scripts = {
    # run with watching
    dev.exec = "cargo watch -x run";
    
    # run tests with watching
    test-watch.exec = "cargo watch -x test";
    
    # run with backtrace
    run-debug.exec = "RUST_BACKTRACE=full cargo run";
    
    # check everything
    check-all.exec = ''
      cargo fmt --check &&
      cargo clippy -- -D warnings &&
      cargo test &&
      cargo audit
    '';
  };
}
