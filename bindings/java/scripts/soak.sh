#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
durations="${OMQ_JAVA_SOAK_DURATIONS:-300 600 1800 3600}"
workers="${OMQ_JAVA_SOAK_WORKERS:-$(nproc)}"

for duration in ${durations}; do
  echo "== OMQ.java soak: ${duration}s, workers=${workers} =="
  OMQ_JAVA_SOAK=1 \
  OMQ_JAVA_SOAK_DURATION_SECS="${duration}" \
  OMQ_JAVA_SOAK_WORKERS="${workers}" \
    mvn -f "${repo_root}/bindings/java/pom.xml" -Dtest=JavaSoakTest test
done
