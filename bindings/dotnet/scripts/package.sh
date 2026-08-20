#!/usr/bin/env bash
set -euo pipefail

if (($# < 2)); then
    echo "usage: $0 OUTPUT_DIR RID=LIBRARY [RID=LIBRARY ...]" >&2
    exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
project="$repo_root/bindings/dotnet/Omq.Net.csproj"
output_dir=$1
shift
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT

for asset in "$@"; do
    rid=${asset%%=*}
    library=${asset#*=}
    if [[ "$rid" == "$asset" || -z "$rid" || -z "$library" ]]; then
        echo "invalid native asset: $asset" >&2
        exit 2
    fi
    mkdir -p "$stage/$rid"
    cp "$library" "$stage/$rid/"
done

mkdir -p "$output_dir"
dotnet pack "$project" --configuration Release --no-restore \
    --output "$output_dir" -p:NativeAssetRoot="$stage"
