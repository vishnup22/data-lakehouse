# Contributing

Thanks for reviewing this project. It is primarily a portfolio demonstration, but issues and improvements are welcome.

## Local development

```bash
cp .env.example .env
pip install -r requirements.txt -r requirements-dev.txt
make ci          # ruff + mypy + pytest (no Docker required)
```

## Code style

- **Formatter / linter:** [Ruff](https://docs.astral.sh/ruff/) — `make lint`
- **Types:** [mypy](https://mypy-lang.org/) on `src/` — `make typecheck`
- **Tests:** pytest — `make test`

Configuration lives in [`pyproject.toml`](pyproject.toml).

## Running the full stack

Interview demo (~20 min):

```bash
make demo-env
make up
make produce
make stream      # ~10 min, then Ctrl+C
make wait-eligible
make benchmark-before
make compact-once
make benchmark-after
make benchmark-compare
```

See [README.md](README.md) for troubleshooting.

## Pull requests

1. Fork the repository and create a feature branch.
2. Run `make ci` before opening a PR.
3. Keep changes focused — this repo values clear, production-shaped patterns over feature breadth.
4. Update docs if you change compaction rules, env vars, or CLI behavior.

## Reporting issues

Include:

- Command you ran (`make stream`, `make compact-once`, etc.)
- Relevant log output or exit code
- OS and Docker version

Do not commit `.env` files or credentials.
