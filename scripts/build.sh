#!/usr/bin/env bash

# Stop if any process returns non-zero exit code
set -e

echo "Removing existing build..."
rm -rf ./bin && mkdir ./bin
cp -r ./src/public ./bin/public
cp -r ./src/views ./bin/views
cp -r ./src/tests ./bin/tests
mkdir ./bin/routes
mkdir -p /countdera/output

# Compile all coffeescript to js
echo "Compiling Coffeescript to JS..."
./node_modules/.bin/coffee --output ./bin/ --compile ./src/

./node_modules/.bin/browserify --transform coffeeify --debug  \
./src/public/javascripts/index.coffee > ./bin/public/javascripts/bundle.js

echo "Linting..."
find ./src -name "*.coffee" -print0 | xargs -0 ./node_modules/.bin/coffeelint -f ./coffeelint.json

echo "Build successful!"
