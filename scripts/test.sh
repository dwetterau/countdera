#!/usr/bin/env bash

# Stop if any process returns non-zero exit code
set -e

# Sanity check to make sure we're being run from project root
if [ "$0" != "./scripts/test.sh" ]; then
    echo "Start failed: Wrong working directory"
    echo "You need to be in the project root to run this script"
    exit 1
fi

# Run the build script
chmod +x ./scripts/build.sh && ./scripts/build.sh

# Create the master test bundle
echo "Making test bundles..."
./node_modules/.bin/browserify --transform coffeeify --debug  \
./src/tests/all_tests.coffee > ./bin/tests/all_tests.js

echo "Running tests..."
find ./bin -name "all_tests.js" -print0 | xargs -0 ./node_modules/.bin/mocha --reporter spec