#!/bin/bash

docker run -it --rm \
  -e GITHUB_TOKEN= \
  -e GITHUB_REPO=harperdb/rocksdb-js \
  --name github-runner \
  github-runner
