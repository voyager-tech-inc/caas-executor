<!--
SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.

SPDX-License-Identifier: MIT
-->
# Compression as a Service (CaaS) Executor

Compression as a Service (CaaS) is a tool used to compress files using existing command line
utilities.

![Infographic about how the CaaS executor works](./docs/imgs/CaaS_Flow.png)

Basically, the app comes up and monitors some input directory. As files are added to the input,
they are compressed using the tool of your choice and output to an output directory.

There are two main modes:

1. Server: the app persistently looks at the input directory and processes new files as they
   arrive.
2. One-shot: the app starts up, processes all files in the input directory, then exits.

## Building

This project uses [Earthly](https://earthly.dev/earthfile) to build. After installing Earthly,
run `earthly -P +all` to build the container and run tests. It's output as:

```
ghcr.io/voyager-tech-inc/caas-executor:latest
```

You can also pull the latest image directly from GitHub via `docker pull` or equivalent.

## Running

You can see all command line arguments available by running:

```
docker run --rm ghcr.io/voyager-tech-inc/caas-executor:latest --help
```

## Database migrations and testing

This app uses PostgreSQL to track runtime state. To prepare a new database, you can use
the [`migrations`](./migrations) and the [SQLx CLI](https://crates.io/crates/sqlx-cli).
Note: the app will assert database schema on startup using these migrations, so you don't
need to do this step ahead of time.

```sh
# Install the SQLx CLI
cargo install sqlx-cli --no-default-features --features native-tls,postgres

# Bring up your database (if needed)
docker run --name postgres -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres

# Set the database URL
export DATABASE_URL=postgres://postgres:mysecretpassword@localhost:5432/postgres

# Run the migrations (this populates the database with the app's schema)
cargo sqlx migrate run

# Prepare the offline query metadata (run when schema changes)
cargo sqlx prepare -- --tests

# Annotate the JSON headers if necessary
reuse annotate -c "Voyager Technologies, Inc." -l "MIT" .sqlx/query*.json

# Run tests against the live database
cargo test
```
