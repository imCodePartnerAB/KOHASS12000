#!/bin/bash
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

if pgrep -f "imcode_ss12000.pl" > /dev/null; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') : Script already running. Exiting."
    exit 1
fi

script_paths=$(find /var/lib/koha -type f -name "imcode_ss12000.pl" | grep "Plugin/imCode/KohaSS12000/ExportUsers/cron")
script_count=$(echo "$script_paths" | wc -l)

if [ $script_count -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') : Script not found"
    exit 1
fi

start_time=$(date +%s)
max_iterations=100  
max_empty_iterations=5  

while IFS= read -r script_path; do
    instance=$(echo "$script_path" | cut -d/ -f5)
    echo "$(date '+%Y-%m-%d %H:%M:%S') : Processing instance $instance"
    
    i=1
    empty_iterations=0
    
    while [ $i -le $max_iterations ]; do
        output=$(timeout 10m sudo koha-shell "$instance" -c "$script_path" 2>&1)
        
        if echo "$output" | grep -q "EndLastPageFromAPI"; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') : Finished processing for $instance"
            break
        fi
        
        if echo "$output" | grep -q "ErrorVerifyCategorycodeBranchcode"; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') : Error occurred for $instance"
            break
        fi
        
        if [ -z "$output" ]; then
            empty_iterations=$((empty_iterations + 1))
            if [ $empty_iterations -ge $max_empty_iterations ]; then
                echo "$(date '+%Y-%m-%d %H:%M:%S') : Too many empty iterations for $instance"
                break
            fi
        else
            empty_iterations=0
        fi
        
        echo "$(date '+%Y-%m-%d %H:%M:%S') : Iteration $i for $instance"
        
        i=$((i+1))
        
        [ $(($(date +%s) - start_time)) -gt 28800 ] && { 
            echo "$(date '+%Y-%m-%d %H:%M:%S') : Exceeded 8 hours. Terminating."; 
            exit 1; 
        }
    done
    
done <<< "$script_paths"