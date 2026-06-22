#!/usr/bin/env bash

wait_for_service_registration() {
    local db_path="$1"
    local realm_id="$2"
    local manufacturer="$3"
    local actor_name="$4"
    local timeout_seconds="${5:-60}"
    local poll_interval="${SERVICE_READY_POLL_INTERVAL_SECONDS:-1}"
    local deadline=$((SECONDS + timeout_seconds))

    if [[ ! "$realm_id" =~ ^[0-9]+$ ]]; then
        echo "Invalid realm id: $realm_id" >&2
        return 2
    fi
    if [[ ! "$manufacturer" =~ ^[A-Za-z0-9_.-]+$ ]] || [[ ! "$actor_name" =~ ^[A-Za-z0-9_.-]+$ ]]; then
        echo "Invalid actor type: ${manufacturer}:${actor_name}" >&2
        return 2
    fi

    while [ "$SECONDS" -lt "$deadline" ]; do
        if [ -f "$db_path" ]; then
            local service_count
            service_count="$(
                sqlite3 "$db_path" "
                    SELECT COUNT(*)
                    FROM service_registry
                    WHERE actor_realm_id = ${realm_id}
                      AND actor_manufacturer = '${manufacturer}'
                      AND actor_device_name = '${actor_name}'
                      AND service_name = '${manufacturer}:${actor_name}'
                      AND status = 'Available';
                " 2>/dev/null || true
            )"
            if [[ "$service_count" =~ ^[0-9]+$ ]] && [ "$service_count" -gt 0 ]; then
                return 0
            fi
        fi

        sleep "$poll_interval"
    done

    return 1
}
