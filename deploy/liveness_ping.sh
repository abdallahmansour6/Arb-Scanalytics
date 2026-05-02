#!/bin/bash
# Collector liveness check.
#
# Verifies the collector has written a parquet file within the last
# $THRESHOLD_MIN minutes. If not, sends a Pushover alert (sourcing
# credentials from the scanner's own .env so we don't depend on
# unrelated projects' configs).
#
# Cooldown: $ALERT_COOLDOWN_MIN minutes between alerts during an
# extended outage (so we don't spam Pushover). When a fresh write is
# observed, the cooldown state is cleared so the next outage triggers
# immediately.
#
# Cron (every 5 min):
#   */5 * * * * /opt/Arb-Scanalytics/liveness_ping.sh

set -eu

DATA_DIR="${DATA_DIR:-/opt/Arb-Scanalytics/data}"
THRESHOLD_MIN="${THRESHOLD_MIN:-5}"
ALERT_STATE="${ALERT_STATE:-/tmp/arb-collector-alerted}"
ALERT_COOLDOWN_MIN="${ALERT_COOLDOWN_MIN:-60}"
PROJECT_ENV="${PROJECT_ENV:-/opt/Arb-Scanalytics/.env}"

# Pushover credentials. Prefer environment if already set; otherwise
# extract directly from the project's .env. We grep instead of `source`
# because the .env can contain CRLF line endings and `source` would
# choke on the embedded \r. The regexes tolerate quoted values.
read_env_var() {
    local key="$1"
    grep -E "^${key}=" "$PROJECT_ENV" 2>/dev/null \
        | head -1 \
        | sed -E "s/^${key}=//; s/^['\"]?//; s/['\"]?\r?\$//"
}

PUSHOVER_TOKEN="${PUSHOVER_TOKEN:-}"
PUSHOVER_USER="${PUSHOVER_USER:-}"
if [[ -z "$PUSHOVER_TOKEN" && -f "$PROJECT_ENV" ]]; then
    PUSHOVER_TOKEN=$(read_env_var PUSHOVER_TOKEN)
fi
if [[ -z "$PUSHOVER_USER" && -f "$PROJECT_ENV" ]]; then
    PUSHOVER_USER=$(read_env_var PUSHOVER_USER)
fi

NOW=$(date +%s)

# Newest parquet's mtime. find -newermt is what we'd prefer, but we
# need the actual age for the alert message — so sort -rn works fine.
LATEST_TS=$(find "$DATA_DIR" -name "*.parquet" -printf "%T@\n" 2>/dev/null \
            | sort -rn | head -1 | cut -d'.' -f1)

if [[ -z "${LATEST_TS:-}" ]]; then
    AGE_MIN=99999
else
    AGE_MIN=$(( (NOW - LATEST_TS) / 60 ))
fi

if (( AGE_MIN > THRESHOLD_MIN )); then
    # Stalled. Honor cooldown.
    if [[ -f "$ALERT_STATE" ]]; then
        LAST_ALERT_TS=$(stat -c %Y "$ALERT_STATE" 2>/dev/null || echo 0)
        if (( (NOW - LAST_ALERT_TS) / 60 < ALERT_COOLDOWN_MIN )); then
            exit 0
        fi
    fi

    if [[ -z "$PUSHOVER_TOKEN" || -z "$PUSHOVER_USER" ]]; then
        echo "$(date -Is) STALE: ${AGE_MIN}m old, but PUSHOVER creds missing" >&2
        exit 1
    fi

    HOST=$(hostname)
    MSG="Arb-Scanalytics collector appears stalled. Latest parquet write: ${AGE_MIN}m ago (threshold: ${THRESHOLD_MIN}m). VPS: ${HOST}."

    curl -sS --max-time 10 \
        --form-string "token=$PUSHOVER_TOKEN" \
        --form-string "user=$PUSHOVER_USER" \
        --form-string "title=Scanner stalled" \
        --form-string "message=$MSG" \
        --form-string "priority=1" \
        https://api.pushover.net/1/messages.json > /dev/null

    touch "$ALERT_STATE"
    echo "$(date -Is) ALERT sent: stale ${AGE_MIN}m" >&2
    exit 1
fi

# Fresh: clear any prior alert state.
rm -f "$ALERT_STATE"
