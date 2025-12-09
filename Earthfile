# SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
#
# SPDX-License-Identifier: MIT

VERSION 0.8

IMPORT github.com/earthly/lib/rust:3.0.3 AS rust

all:
    BUILD +lint
    BUILD +test
    BUILD +caas-executor
    BUILD +vendor-tarball

lint:
    BUILD +reuse
    BUILD +typos
    BUILD +clippy
    BUILD +rustfmt
    BUILD +shellcheck
    BUILD +audit
    BUILD +outdated
    BUILD +unused-deps

test:
    BUILD +unit-test
    BUILD +integration-test

rust-base:
    ARG BUILD_REGISTRY=registry.access.redhat.com
    ARG BUILD_IMAGE=ubi9/ubi
    ARG BUILD_TAG=9.7

    FROM ${BUILD_REGISTRY}/${BUILD_IMAGE}:${BUILD_TAG}

    ENV CARGO_HOME=/opt/rust/src/.cargo
    ENV PATH=/opt/rust/src/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

    RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm && \
        dnf update --setopt=tsflags=nodocs -y && \
        dnf install -y --nodocs gcc \
                                openssl-devel \
                                zlib-devel \
                                perl \
                                inotify-tools && \
        dnf clean all

    RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
            | sh -s -- --default-toolchain none -y && \
        rustup toolchain install stable && \
        cargo install cargo-audit \
                      cargo-outdated \
                      cargo-machete \
                      cargo-license \
                      cargo-sweep && \
        rm -rf "${CARGO_HOME}"/registry/{cache,src}

rust-src:
    FROM +rust-base
    WORKDIR /build
    DO rust+INIT --keep_fingerprints=true
    COPY --keep-ts --dir src .sqlx Cargo.lock Cargo.toml ./

clippy:
    FROM +rust-src
    DO rust+CARGO --args="clippy --all-features -- -D warnings"

rustfmt:
    FROM +rust-src
    DO rust+CARGO --args="fmt --check"

audit:
    FROM +rust-src
    # Note: ignoring RUSTSEC-2023-0071 as it's a false positive in our configuration:
    # https://github.com/launchbadge/sqlx/issues/2911
    DO rust+CARGO --args="--frozen audit --ignore RUSTSEC-2023-0071"

outdated:
    FROM +rust-src
    DO rust+CARGO --args="--frozen outdated --root-deps-only --exit-code 0 -v"

unused-deps:
    FROM +rust-src
    DO rust+CARGO --args="machete --with-metadata"

license-manifest:
    FROM +rust-src
    DO rust+CARGO --args="--frozen license --avoid-build-deps --avoid-dev-deps -d -j -o rust_dependency_manifest.json"
    DO rust+CARGO --args="--frozen license --avoid-build-deps --avoid-dev-deps -d -t -o rust_dependency_manifest.tsv"
    SAVE ARTIFACT ./rust_dependency_manifest.*

build-bin:
    FROM +rust-src
    DO rust+CARGO --args="build --release --locked" --output="release/[^/\.]+"
    SAVE ARTIFACT target/release/caas-executor caas-executor

vendor-tarball:
    FROM +rust-src
    RUN dnf update && \
        dnf install -y --nodocs xz && \
        dnf clean all
    DO rust+CARGO --args="vendor --locked target/vendor" --output="vendor/.*"
    RUN tar -cJf caas-executor-vendored-deps.tar.xz -C target vendor
    SAVE ARTIFACT caas-executor-vendored-deps.tar.xz AS LOCAL ./

unit-test-image:
    FROM +build-bin
    COPY --dir ./migrations ./
    DO rust+CARGO --args="build --tests --release" --output="release/deps/caas_executor-[^\./]+"
    RUN cp -a ./target/release/deps/caas_executor-* ./caas_executor_test
    ENTRYPOINT ["./caas_executor_test"]

caas-executor:
    FROM docker.io/library/archlinux:latest
    COPY +build-bin/caas-executor ./
    LET VERSION = $(./caas-executor --version | cut -d ' ' -f 2)
    ARG EARTHLY_GIT_REFS
    ARG EARTHLY_GIT_SHORT_HASH
    # If there isn't a current Git ref matching the version, add a git tag
    LET DEV = $(printf "%s" "$EARTHLY_GIT_REFS" | grep -q -w "$VERSION" || printf -- "-git-%s-%s" "$(date +%Y%m%d)" "$EARTHLY_GIT_SHORT_HASH")
    SET VERSION = ${VERSION}${DEV}
    BUILD +caas-executor-img --VERSION=${VERSION}

caas-executor-img:
    ARG BASE_REGISTRY=registry.access.redhat.com
    ARG BASE_IMAGE=ubi9/ubi
    ARG BASE_TAG=9.7

    FROM ${BASE_REGISTRY}/${BASE_IMAGE}:${BASE_TAG}

    RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm && \
        dnf install -y --nodocs inotify-tools && \
        dnf clean all && \
        useradd -u 1006 -m caas-user

    ENV COMPRESSION_ALGORITHM=Gzip
    WORKDIR /app
    COPY --dir ./migrations ./
    COPY +build-bin/caas-executor /usr/bin/
    ENTRYPOINT ["/usr/bin/caas-executor"]
    CMD ["-h"]

    ARG --required VERSION
    SAVE IMAGE --push ghcr.io/voyager-tech-inc/caas-executor:latest \
                      ghcr.io/voyager-tech-inc/caas-executor:${VERSION}

unit-test:
    FROM docker.io/library/docker:latest
    WORKDIR /test
    WITH DOCKER --load unit-test-image:latest=(+unit-test-image) \
                --pull docker.io/library/postgres:latest
        RUN docker run --rm --name pg \
                       -e POSTGRES_DB=caas \
                       -e POSTGRES_USER=caas \
                       -e POSTGRES_PASSWORD=test \
                       -d docker.io/library/postgres:latest > /dev/null && \
            sleep 5 && \
            docker run -e DATABASE_URL="postgres://caas:test@127.0.0.1/caas" \
                       --network=container:pg \
                       --rm \
                       unit-test-image:latest
    END

integration-test:
    FROM docker.io/library/docker:latest
    WORKDIR /test
    COPY ./compose/compose.yml ./
    COPY ./scripts/test_service.sh ./
    WITH DOCKER --load ghcr.io/voyager-tech-inc/caas-executor:latest=(+caas-executor-img --VERSION=test) \
                --pull docker.io/library/postgres:latest \
                --compose compose.yml
        RUN ./test_service.sh
    END

typos:
    FROM docker.io/library/archlinux:latest
    RUN pacman -Sy --noconfirm typos
    WORKDIR /src
    COPY --dir . ./
    RUN typos

reuse:
    FROM docker.io/fsfe/reuse:latest
    WORKDIR /src
    COPY --dir . /src
    RUN --network=none reuse lint

shellcheck:
    FROM registry.gitlab.com/pipeline-components/shellcheck:latest
    COPY **/*.sh .
    RUN --network=none shellcheck ./*.sh
