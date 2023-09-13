#!/bin/bash
i=1
while true; do
    echo "Running iteration $i"
    output=$(sudo koha-foreach --chdir --enabled /usr/share/koha/bin/cronjobs/imcode_ss12000.pl 2>&1)

    if echo "$output" | grep -q "EndLastPageFromAPI"; then
        echo "Received 'EndLastPageFromAPI'. Exiting loop."
        break
    fi

    echo "Iteration $i completed"
    i=$((i+1))
done
