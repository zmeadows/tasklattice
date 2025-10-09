# TaskLattice

![CI](https://github.com/zmeadows/tasklattice/actions/workflows/ci.yml/badge.svg)

> ⚠️ **This project is under active development and not yet ready for public use.**
> The APIs, behavior, and structure may change significantly until a stable release.

TaskLattice generates many input variants from Python-defined parameters and renders templated files for each run. It launches jobs through simple runners (Local, Slurm) and keeps sweep logic separate from execution. Results are easy to iterate over, with lightweight output-file checks and optional caching/resume.

## Development

TaskLattice targets **Python 3.11 and 3.12**. Daily development is on **3.11**.

### Quick start (3.11)

```bash
# From repo root
python3.11 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -e ".[dev]"

# Lint, type-check, test
ruff check .
mypy .
pytest

# install hooks (uses tools from the active venv)
pre-commit install
pre-commit install --hook-type pre-push

# commit-stage hooks: ruff + mypy
pre-commit run --all-files

# pre-push stage: pytest -q
pre-commit run --all-files --hook-stage pre-push
```

