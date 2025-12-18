#!/bin/sh
#
# Entrypoint script for DBP ETL Dev Container
# Sets up the runtime environment and keeps the container running

set -e

# Display ready message
echo ""
echo "=== DBP ETL Dev Container Started ==="
echo ""

# Keep container running for interactive use
# This prevents the container from exiting immediately
tail -f /dev/null
