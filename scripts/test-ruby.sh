#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
ruby="${OMQ_RUBY:-ruby}"

if ! command -v "$ruby" >/dev/null 2>&1; then
    echo "Ruby binding tests require ruby; set OMQ_RUBY=/path/to/ruby" >&2
    exit 1
fi

ruby="$(command -v "$ruby")"
export PATH="$(dirname "$ruby"):$PATH"
export RUBY="$ruby"

cd "$repo_root/bindings/ruby"
"$ruby" -S bundle check
"$ruby" -S bundle exec rake
