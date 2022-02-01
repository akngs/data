#!/usr/bin/env bash
rm -rf build && mkdir -p build
touch build/.nojekyll

deno run --allow-net --allow-write src/index.ts
