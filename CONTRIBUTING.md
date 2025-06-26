# Development Workflow

#### Initial Setup

Install the required dependencies:

```bash
./install-dependencies
```

Please start a new terminal session after running the install-dependencies script, and run the below commands in the new
terminal session. During development, you'll commonly use these commands:

#### Check

For quick syntax and type checking:

```bash
just check
```

This runs `cargo check` which is much faster than `cargo build` since it skips producing executables. Use this
frequently while coding to catch errors early.

#### Build

To compile the project:

```bash
just build
```

#### Code Quality

To maintain code quality, run these in sequence:

Run the linter:

```bash
just lint
```

Format the code:

```bash
just format
```

Note: Always run `just format` after linting since linter fixes may not follow formatting rules.

#### Testing

Run the test suite:

```bash
just test
```

Generate test coverage reports:

```bash
just coverage
```

#### Git Hooks

Install the git hooks which will run the linter and formatter before each commit, if you want to skip the hooks, you can
use `git commit --no-verify`,

```bash
git config --global --add safe.directory $(pwd)
git config core.hooksPath .husky/_
```

or remove the hooks if pre-commit hooks block your workflow by running:

```bash
git config --unset core.hooksPath
```
