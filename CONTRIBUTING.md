# Contributing to Unirust

Thank you for your interest in contributing to Unirust! This document provides guidelines and information for contributors.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct. We are committed to providing a welcoming and inspiring community for all.

## Getting Started

### Prerequisites

- Rust 1.70 or later
- Git
- Basic familiarity with entity resolution, temporal modeling, or graph algorithms (helpful but not required)

### Development Setup

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/your-username/unirust.git
   cd unirust
   ```
3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/unirust/unirust.git
   ```
4. Create a new branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

### Building and Testing

```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run specific test modules
cargo test conflicts::tests
cargo test dsu::tests

# Run benchmarks
cargo bench

# Check code formatting
cargo fmt --check

# Run clippy for linting
cargo clippy --all-targets --all-features
```

## How to Contribute

### Types of Contributions

We welcome several types of contributions:

- **Bug fixes**: Fix issues in existing functionality
- **Feature additions**: Add new capabilities to the library
- **Performance improvements**: Optimize existing algorithms
- **Documentation**: Improve docs, examples, or comments
- **Tests**: Add test coverage for new or existing functionality
- **Examples**: Create new example applications

### Development Workflow

1. **Plan your contribution**: Open an issue to discuss significant changes
2. **Write code**: Follow our coding standards (see below)
3. **Write tests**: Ensure your code is well-tested
4. **Update documentation**: Update relevant docs and examples
5. **Submit a pull request**: Follow our PR guidelines

### Coding Standards

#### Rust Style

- Follow standard Rust formatting with `cargo fmt`
- Use `cargo clippy` to catch common issues
- Prefer explicit error handling over panics
- Use meaningful variable and function names
- Add doc comments for public APIs

#### Code Organization

- Keep modules focused and cohesive
- Use appropriate visibility modifiers (`pub`, `pub(crate)`, etc.)
- Prefer composition over inheritance
- Keep functions small and focused

#### Performance Considerations

- Consider performance implications of new features
- Add benchmarks for performance-critical code
- Use appropriate data structures for the use case
- Consider memory usage and allocation patterns

### Testing Guidelines

#### Unit Tests

- Write unit tests for all public APIs
- Test edge cases and error conditions
- Use descriptive test names that explain the scenario
- Group related tests in modules

#### Integration Tests

- Test complete workflows and examples
- Verify that different modules work together correctly
- Test with realistic data sizes and patterns

#### Property-Based Testing

- Use property-based testing for complex algorithms (we use `proptest`)
- Test invariants and properties that should always hold
- Focus on temporal correctness and entity resolution properties

### Documentation

#### Code Documentation

- Add doc comments for all public APIs
- Use examples in doc comments where helpful
- Document performance characteristics and complexity
- Explain temporal semantics and edge cases

#### Example Documentation

- Keep examples up-to-date with API changes
- Add comments explaining the business logic
- Show both simple and complex use cases
- Include expected outputs where relevant

### Pull Request Guidelines

#### Before Submitting

- [ ] Code compiles without warnings
- [ ] All tests pass (`cargo test`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] No clippy warnings (`cargo clippy`)
- [ ] Documentation is updated
- [ ] New features have tests
- [ ] Performance-critical changes have benchmarks

#### PR Description

- Provide a clear description of what the PR does
- Link to any related issues
- Explain any breaking changes
- Include performance impact if relevant
- Add screenshots for UI changes (if applicable)

#### Review Process

- All PRs require at least one review
- Address review feedback promptly
- Keep PRs focused and reasonably sized
- Rebase on main before final merge (we may squash commits)

### Issue Guidelines

#### Bug Reports

When reporting bugs, please include:

- Clear description of the issue
- Steps to reproduce
- Expected vs actual behavior
- Environment details (Rust version, OS, etc.)
- Minimal code example if possible

#### Feature Requests

For feature requests, please include:

- Clear description of the desired functionality
- Use case and motivation
- Proposed API design (if applicable)
- Alternative solutions considered

### Performance Contributions

Unirust is designed for high performance. When contributing performance improvements:

- Add benchmarks to measure impact
- Consider both time and space complexity
- Test with realistic data sizes
- Document performance characteristics
- Consider trade-offs with code complexity

### Temporal Modeling Contributions

The temporal aspects of Unirust are critical. When working with temporal features:

- Understand Allen's interval relations
- Consider edge cases in temporal logic
- Test with various interval configurations
- Document temporal semantics clearly
- Ensure temporal correctness in tests

### Entity Resolution Contributions

When contributing to entity resolution:

- Understand the blocking algorithm
- Consider identity key and strong identifier semantics
- Test with various conflict scenarios
- Consider perspective weighting
- Document resolution logic clearly

## Development Tools

### Recommended VS Code Extensions

- rust-analyzer
- CodeLLDB (for debugging)
- Better TOML
- GitLens

### Useful Commands

```bash
# Generate dependency graph
cargo tree

# Check for security vulnerabilities
cargo audit

# Run tests with coverage (requires cargo-tarpaulin)
cargo tarpaulin --out html

# Check documentation
cargo doc --no-deps --open
```

## Release Process

Releases are managed by maintainers. If you need a new release:

1. Open an issue requesting a release
2. Include a summary of changes since the last release
3. Highlight any breaking changes
4. Suggest version bump (patch/minor/major)

## Getting Help

- Open an issue for questions or problems
- Check existing issues and discussions
- Join our community discussions (if available)
- Review the documentation and examples

## License

By contributing to Unirust, you agree that your contributions will be licensed under the MIT License.

## Thank You

Thank you for contributing to Unirust! Your contributions help make entity resolution more accessible and reliable for everyone.
