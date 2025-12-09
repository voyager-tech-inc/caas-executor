#!/bin/sh
# SPDX-FileCopyrightText: 2025 Voyager Technologies, Inc.
#
# SPDX-License-Identifier: MIT

set -o errexit

printf "Adding file to compression input...\n"
f=$(mktemp)
dd if=/dev/urandom of="$f" bs=4M count=1
target_md5="$(md5sum "$f" | cut -d ' ' -f 1)"

mv "$f" "${PWD}/comp_input/random.dat"

printf "Waiting for CaaS to process file..."
t=0
timeout=30
until [ -f "${PWD}/decomp_output/random.dat" ]; do
    sleep 1
    printf "."
    t=$((t + 1))
    if [ "$t" -ge "$timeout" ]; then
        printf "\nerror - timed out waiting for CaaS file\n" >&2
        exit 1
    fi
done

printf " DONE\n"

if [ "$(md5sum "${PWD}/decomp_output/random.dat" | cut -d ' ' -f 1)" = "$target_md5" ]; then
    printf "SUCCESS: checksums match\n"
    exit 0
else
    printf "FAILURE: checksums do not match\n" >&2
    exit 1
fi
