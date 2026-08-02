#!/usr/bin/env bash
# Run one long CI command. If it exceeds the timeout, dump enough local
# process state for hang diagnosis before terminating the command tree.
set -euo pipefail

usage() {
    echo "usage: $0 <label> <timeout-seconds> -- <command> [args...]" >&2
}

if [[ $# -lt 3 ]]; then
    usage
    exit 2
fi

label=$1
timeout_seconds=$2
shift 2

if [[ "${1:-}" == "--" ]]; then
    shift
fi

if [[ $# -eq 0 ]]; then
    usage
    exit 2
fi

cmd=("$@")
start_epoch=$(date +%s)
safe_label="$(printf '%s' "$label" | tr -c 'A-Za-z0-9_.-' '_')"
forensics_dir="${OMQ_FORENSICS_DIR:-target/hang-forensics}"
mkdir -p "$forensics_dir"
log_file="$forensics_dir/${safe_label}.log"
: >"$log_file"

log() {
    printf '%s\n' "$*" | tee -a "$log_file"
}

run_capture() {
    local title=$1
    shift
    log "--- $title ---"
    "$@" 2>&1 | tee -a "$log_file" || true
}

dump_proc_stacks() {
    [[ -d /proc ]] || return 0

    log "--- proc stacks ---"
    local pids=()
    if command -v pgrep >/dev/null 2>&1; then
        mapfile -t pids < <(
            pgrep -f 'cargo|nextest|target/.*/deps|cross|qemu|pytest|python|miri' \
                2>/dev/null || true
        )
    fi

    if [[ ${#pids[@]} -eq 0 ]]; then
        log "no matching pids"
        return 0
    fi

    local pid task tid cmdline wchan
    for pid in "${pids[@]}"; do
        [[ "$pid" =~ ^[0-9]+$ ]] || continue
        [[ -d "/proc/$pid" ]] || continue
        cmdline="$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)"
        log "pid=$pid cmd=${cmdline:-unknown}"

        if [[ -d "/proc/$pid/task" ]]; then
            for task in /proc/"$pid"/task/*; do
                [[ -d "$task" ]] || continue
                tid="${task##*/}"
                wchan="$(cat "$task/wchan" 2>/dev/null || true)"
                log "tid=$tid wchan=${wchan:-unknown}"
                if [[ -r "$task/stack" ]]; then
                    cat "$task/stack" 2>/dev/null | tee -a "$log_file" || true
                else
                    log "stack unreadable"
                fi
            done
        elif [[ -r "/proc/$pid/stack" ]]; then
            cat "/proc/$pid/stack" 2>/dev/null | tee -a "$log_file" || true
        else
            log "stack unreadable"
        fi
    done
}

dump_forensics() {
    local reason=$1
    local now elapsed
    now=$(date +%s)
    elapsed=$((now - start_epoch))

    log "::group::hang forensics: $label"
    log "reason=$reason"
    log "label=$label"
    log "elapsed_seconds=$elapsed"
    log "timeout_seconds=$timeout_seconds"
    printf 'command:' | tee -a "$log_file"
    printf ' %q' "${cmd[@]}" | tee -a "$log_file"
    printf '\n' | tee -a "$log_file"
    run_capture "date" date -u
    run_capture "uname" uname -a
    run_capture "process tree" ps -eo pid,ppid,pgid,sid,stat,etime,comm,args --forest
    if command -v pgrep >/dev/null 2>&1; then
        run_capture "matching processes" \
            pgrep -af 'cargo|nextest|target/.*/deps|cross|qemu|pytest|python|miri'
    fi
    dump_proc_stacks
    log "::endgroup::"
}

kill_tree() {
    local pid=$1
    local child
    for child in $(pgrep -P "$pid" 2>/dev/null || true); do
        kill_tree "$child"
    done
    kill -TERM "$pid" 2>/dev/null || true
}

"${cmd[@]}" &
child=$!

while kill -0 "$child" 2>/dev/null; do
    now=$(date +%s)
    elapsed=$((now - start_epoch))
    if ((elapsed >= timeout_seconds)); then
        dump_forensics "timeout"
        kill_tree "$child"
        sleep 5
        kill -KILL "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
        exit 124
    fi
    sleep 1
done

set +e
wait "$child"
rc=$?
set -e
exit "$rc"
