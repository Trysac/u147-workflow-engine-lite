# workflow-engine-lite

[![PyPI version](https://img.shields.io/pypi/v/trysac-workflow-engine-lite)](https://pypi.org/project/trysac-workflow-engine-lite/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-workflow-engine-lite)](LICENSE)
[![CI](https://github.com/Trysac/u147-workflow-engine-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u147-workflow-engine-lite/actions/workflows/ci.yml)

Durable workflow instances as validated state machines: persisted state, event transitions, timeout timers fired by tick, event dedup and optimistic concurrency

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-workflow-engine-lite
# or
uv add trysac-workflow-engine-lite
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import workflow_engine_lite

# normalize() trims surrounding whitespace and lower-cases the text.
print(workflow_engine_lite.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    workflow_engine_lite.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
