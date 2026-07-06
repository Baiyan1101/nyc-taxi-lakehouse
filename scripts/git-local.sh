#!/usr/bin/env bash

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

exec git --git-dir="$PROJECT_ROOT/.repo.git" --work-tree="$PROJECT_ROOT" "$@"

