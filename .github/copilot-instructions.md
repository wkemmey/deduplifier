# Copilot Instructions for deduplifier

## Project Overview

deduplifier is a Rust project for identifying and managing duplicate files.

## Technology Stack

- **Language**: Rust
- **Package Manager**: Cargo
- **Testing**: Cargo test framework
- **Code Quality**: rustfmt, clippy

## Development Environment

### Building the Project

```bash
cargo build
```

### Running Tests

```bash
cargo test
```

### Linting and Formatting

```bash
# Format code
cargo fmt

# Run clippy linter
cargo clippy -- -D warnings
```

## Coding Standards

### Rust Best Practices

- Follow Rust 2021 edition conventions
- Use idiomatic Rust patterns
- Prefer explicit error handling with `Result` types over panics
- Use `rustfmt` for consistent code formatting
- Ensure all code passes `clippy` with zero warnings

### Code Style

- Use clear, descriptive variable and function names
- Add doc comments (`///`) for public APIs
- Write unit tests for all new functionality
- Keep functions small and focused on a single responsibility
- Use appropriate visibility modifiers (pub, pub(crate), private)

### Error Handling

- Use custom error types that implement `std::error::Error`
- Provide meaningful error messages
- Propagate errors using `?` operator where appropriate
- Avoid using `unwrap()` and `expect()` in production code; use proper error handling

### Testing

- Write unit tests in the same file as the code (in a `#[cfg(test)]` module)
- Write integration tests in the `tests/` directory
- Aim for high test coverage on critical paths
- Use descriptive test names that explain what is being tested

### Documentation

- Add module-level documentation (`//!`) at the top of each module
- Document all public APIs with doc comments
- Include examples in doc comments where helpful
- Keep README.md up to date with project status and usage

## Git Workflow

- Create feature branches from main
- Write clear, descriptive commit messages
- Keep commits focused and atomic
- Ensure code builds and tests pass before committing

## Boundaries and Constraints

- Never commit secrets, API keys, or sensitive data
- Don't modify `.git` directory or configuration
- Respect the `.gitignore` patterns
- Don't commit generated files (target/, debug/, etc.)

## Common Tasks

### Adding a New Feature

1. Create a new module or function with appropriate visibility
2. Write comprehensive tests
3. Add documentation
4. Run `cargo fmt` and `cargo clippy`
5. Ensure all tests pass with `cargo test`

### Fixing a Bug

1. Add a test that reproduces the bug
2. Fix the bug
3. Verify the test now passes
4. Ensure no other tests are broken

### Refactoring

1. Ensure comprehensive test coverage exists
2. Make changes incrementally
3. Run tests frequently to catch regressions
4. Keep commits small and focused
