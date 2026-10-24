# CI

Every pull request and every push to `main` runs [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml). Nothing in it touches AWS or Jetstream.

| Job | What it checks |
|---|---|
| tests | Installs the locked dependencies with `uv sync --locked` and runs the full test suite |
| terraform | `terraform fmt -check`, then `terraform init -backend=false` and `terraform validate` on `infra/aws/` |

`uv sync --locked` fails if `uv.lock` doesn't match `pyproject.toml`, so a dependency change can't be merged without its lock file.
