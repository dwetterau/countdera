#!/usr/bin/env bash

# Stop if any process returns non-zero exit code
set -e

# Run the build script
chmod +x ./scripts/build.sh && ./scripts/build.sh

if [ "$NODE_ENV" == "" ]; then
    NODE_ENV="local"
fi

echo "Starting $NODE_ENV node configuration..."
echo ""

env "NODE_ENV=$NODE_ENV" node ./bin/index.js
