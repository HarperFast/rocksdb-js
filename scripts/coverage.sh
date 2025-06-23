#!/bin/bash

set -e  # Exit on error

echo "Cleaning previous build..."
rm -rf build/
rm -rf coverage/

echo "Building in debug mode..."
node-gyp rebuild --debug

echo "Verifying build..."
if [ ! -d "build/Debug" ]; then
    echo "Error: Debug build directory not found!"
    exit 1
fi

echo "Running tests..."
pnpm test

echo "Checking for .gcda files..."
find build -name "*.gcda" -type f

echo "Creating coverage directory..."
mkdir -p coverage

echo "Collecting coverage data..."
lcov --ignore-errors unsupported,unsupported,inconsistent --capture --directory build/Debug/obj.target/rocksdb-js/ --output-file coverage/coverage.info

echo "Filtering coverage data to src/binding directory..."
lcov --ignore-errors unsupported,unsupported,inconsistent --extract coverage/coverage.info "*/src/binding/*" --output-file coverage/coverage.info

echo "Generating HTML report..."
genhtml coverage/coverage.info --output-directory coverage/html

echo "Coverage report generated in coverage/html/index.html" 