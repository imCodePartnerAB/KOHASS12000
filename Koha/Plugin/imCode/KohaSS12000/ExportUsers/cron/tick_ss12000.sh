#!/bin/bash
# Run this from cron every minute (one line, see README). It does nothing unless a run is due or has been
# requested on the configuration page, so the old cron lines for run_ss12000.sh keep working unchanged.
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

script_paths=$(find /var/lib/koha -type f -name "imcode_ss12000_tick.pl" 2>/dev/null | grep "Plugin/imCode/KohaSS12000/ExportUsers/cron")
[ -z "$script_paths" ] && exit 0

wrapper="${SS12000_RUN_WRAPPER:-$(dirname "$(readlink -f "$0")")/run_ss12000.sh}"

while IFS= read -r script_path; do
    instance=$(echo "$script_path" | cut -d/ -f5)
    out=$(timeout 30m sudo koha-shell "$instance" -c "$script_path" 2>&1)
    job=$(echo "$out" | grep -o "JOB:[0-9]*:import" | head -1)
    [ -z "$job" ] && continue

    id=$(echo "$job" | cut -d: -f2)
    echo "$(date '+%Y-%m-%d %H:%M:%S') : Import job $id started for $instance"
    wout=$(timeout 8h "$wrapper" "$instance" 2>&1)
    rc=$?
    echo "$wout" | tail -5

    if echo "$wout" | grep -q "Script already running"; then
        timeout 2m sudo koha-shell "$instance" -c "$script_path --release $id" > /dev/null 2>&1
        echo "$(date '+%Y-%m-%d %H:%M:%S') : Import job $id waits, another import is running"
    elif [ $rc -eq 124 ] || echo "$wout" | grep -q "Exceeded 8 hours"; then
        timeout 2m sudo koha-shell "$instance" -c "$script_path --finish $id failed time-limit-8h" > /dev/null 2>&1
    elif echo "$wout" | grep -q "Error occurred"; then
        timeout 2m sudo koha-shell "$instance" -c "$script_path --finish $id failed configuration-error-see-log" > /dev/null 2>&1
    elif echo "$wout" | grep -q "Finished processing for $instance"; then
        timeout 2m sudo koha-shell "$instance" -c "$script_path --finish $id done" > /dev/null 2>&1
        echo "$(date '+%Y-%m-%d %H:%M:%S') : Import job $id finished for $instance"
    else
        timeout 2m sudo koha-shell "$instance" -c "$script_path --finish $id failed stopped-before-the-last-page" > /dev/null 2>&1
    fi
done <<< "$script_paths"
