# azimuth-robotframework

Keyword library for driving Azimuth using [Robot Framework](https://robotframework.org/).

## Running tests

The tests use [pytest](https://docs.pytest.org) and are run with
[tox](https://tox.wiki) via [uv](https://docs.astral.sh/uv/):

```sh
uv run --locked tox                         # lint, tests, mypy and coverage
uv run --locked tox -e 3.12 -- -k <expr>    # run selected tests only
uv run --locked tox -e autofix              # apply ruff and codespell fixes
```

Tests live in `tests/`.
Please add tests alongside your changes.
