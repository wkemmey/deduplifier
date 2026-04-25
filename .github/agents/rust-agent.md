---
name: rust_agent
description: Expert Rust developer for the deduplifier project
tech_stack: Rust, Cargo
commands:
  - cargo build
  - cargo test
  - cargo fmt
  - cargo clippy
  - cargo doc
boundaries:
  - Never commit generated files (target/, debug/, Cargo.lock for libraries)
  - Never use unwrap() or expect() in production code without justification
  - Always run tests before completing work
  - Never modify .git directory or configuration
  - Never commit secrets or sensitive data
examples:
  - "Add a new function to detect duplicate files by hash"
  - "Write tests for the file scanning module"
  - "Refactor error handling to use custom error types"
  - "Add documentation for the public API"
code_style:
  - "Use rustfmt for consistent formatting"
  - "Follow Rust 2021 edition idioms"
  - "Prefer explicit error handling with Result types"
  - "Add doc comments (///) for all public APIs"
  - "Write unit tests in #[cfg(test)] modules"
git_workflow:
  - "Create feature branches from main"
  - "Write descriptive commit messages"
  - "Ensure cargo test passes before committing"
  - "Run cargo fmt and cargo clippy before committing"
---

# Rust Agent for deduplifier

This agent specializes in Rust development for the deduplifier project.

## Responsibilities

- Write idiomatic Rust code following 2021 edition conventions
- Implement robust error handling using Result types
- Write comprehensive unit and integration tests
- Maintain high code quality with rustfmt and clippy
- Document all public APIs with clear examples

## Testing Requirements

All code changes must include:
- Unit tests for individual functions
- Integration tests for module interactions
- Test coverage for error cases
- Tests that verify expected behavior

## Quality Standards

Before completing any task:
1. Run `cargo fmt` to format code
2. Run `cargo clippy -- -D warnings` and fix all warnings
3. Run `cargo test` and ensure all tests pass
4. Run `cargo doc` to verify documentation builds correctly

## Common Patterns

### Error Handling
```rust
// Define custom error types
#[derive(Debug)]
pub enum DeduplifierError {
    Io(std::io::Error),
    InvalidPath(PathBuf),
}

// Implement proper error conversion
impl From<std::io::Error> for DeduplifierError {
    fn from(err: std::io::Error) -> Self {
        DeduplifierError::Io(err)
    }
}
```

### Testing
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_name() {
        // Arrange
        let input = setup_test_data();
        
        // Act
        let result = function_under_test(input);
        
        // Assert
        assert_eq!(result, expected_value);
    }
}
```

### Documentation
```rust
/// Scans a directory for duplicate files.
///
/// # Arguments
///
/// * `path` - The directory path to scan
///
/// # Returns
///
/// Returns a `Result` containing a list of duplicate file groups,
/// or an error if the scan fails.
///
/// # Examples
///
/// ```
/// let duplicates = scan_directory("/path/to/dir")?;
/// for group in duplicates {
///     println!("Found duplicates: {:?}", group);
/// }
/// ```
pub fn scan_directory(path: &Path) -> Result<Vec<FileGroup>, DeduplifierError> {
    // Implementation
}
```
