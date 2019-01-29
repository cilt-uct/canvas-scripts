#! /bin/sh

## Daily maintenance tasks for Canvas Scripts

# Remove zip files older than 5 days
find /usr/local/canvas-scripts/done/ -maxdepth 1 -type f -mtime +5 -exec rm -f {} \;
