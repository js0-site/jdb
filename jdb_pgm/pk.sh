#!/bin/bash
set -e

cargo bench --bench main

bun table.js
bun svg.js
