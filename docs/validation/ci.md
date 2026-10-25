# CI

Every pull request and every push to `main` runs [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml). Nothing in it touches AWS or Jetstream.

| Job | What it checks |
|---|---|
| tests | Sets up Java 21 (the same as the server), installs the locked dependencies with `uv sync --locked`, downloads the Iceberg jars with `deploy/fetch-jars.sh`, and runs the full test suite, Spark and Iceberg tests included |
| terraform | `terraform fmt -check`, then `terraform init -backend=false` and `terraform validate` on `infra/aws/` |

`uv sync --locked` fails if `uv.lock` doesn't match `pyproject.toml`, so a dependency change can't be merged without its lock file.

Java is pinned because the runner's default moves with the image: on Ubuntu 26 it's Java 25, which Spark 4.1 doesn't support.
