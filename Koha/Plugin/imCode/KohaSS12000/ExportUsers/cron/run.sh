#!/bin/bash
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# Check if another instance of the script is already running
if pgrep -f "imcode_ss12000.pl" > /dev/null; then
    echo "Script is already running. Exiting."
    exit 1
fi

# Record the start time of script execution
start_time=$(date +%s)

i=1
while true; do
    # Check the elapsed time
    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))

    if [ $elapsed_time -gt 14400 ]; then  # 4 hours = 4 * 3600 seconds
        echo "Script has been running for more than 4 hours. Forcing termination."
        exit 1
    fi

    echo "Running iteration $i"
    output=$(sudo koha-foreach --chdir --enabled /usr/share/koha/bin/cronjobs/imcode_ss12000.pl 2>&1)

    if echo "$output" | grep -q "EndLastPageFromAPI"; then
        echo "Received 'EndLastPageFromAPI'. Exiting loop."
        break
    fi

    echo "Iteration $i completed"
    i=$((i+1))
done