#!/usr/bin/env bash

# SPDX-License-Identifier: MIT OR Apache-2.0

# Check commit signatures and Conventional Commit subjects in BASE..HEAD.
# Signature checking verifies presence, not signer trust.

set -euo pipefail

BASE=${1:-origin/master}

if ! git rev-parse --verify --quiet "${BASE}^{commit}" >/dev/null; then
    echo "Base revision does not exist: ${BASE}" >&2
    exit 2
fi

mapfile -t COMMITS < <(git rev-list --reverse --no-merges "${BASE}..HEAD")
if [[ ${#COMMITS[@]} -eq 0 ]]; then
    echo "No commits to check"
    exit 0
fi

CONVENTIONAL_PATTERN='^(feat|fix|docs|style|refactor|test|perf|ci|chore|fuzz|bench)(\([a-z0-9._/-]+\))?!?:[[:space:]].+'
FAILURES=0

for COMMIT in "${COMMITS[@]}"; do
    SUBJECT=$(git show --no-patch --format=%s "$COMMIT")

    if [[ ! $SUBJECT =~ $CONVENTIONAL_PATTERN ]]; then
        echo "Invalid commit subject: ${COMMIT} ${SUBJECT}" >&2
        FAILURES=$((FAILURES + 1))
    elif [[ ${#SUBJECT} -gt 72 ]]; then
        echo "Commit subject exceeds 72 characters: ${COMMIT} ${SUBJECT}" >&2
        FAILURES=$((FAILURES + 1))
    fi

    if ! git cat-file commit "$COMMIT" | awk '
        BEGIN { headers = 1; found = 0 }
        /^$/ { headers = 0 }
        headers && /^gpgsig(-sha256)? / { found = 1 }
        END { exit found ? 0 : 1 }
    '; then
        echo "Unsigned commit: ${COMMIT} ${SUBJECT}" >&2
        FAILURES=$((FAILURES + 1))
    fi
done

if [[ $FAILURES -ne 0 ]]; then
    echo "Commit policy failures: ${FAILURES}" >&2
    exit 1
fi

echo "Commit policy passed for ${#COMMITS[@]} commit(s)"
