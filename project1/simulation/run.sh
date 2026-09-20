#!/usr/bin/env bash
set -euo pipefail

output="out"
args=("$@")

if [[ $# -gt 0 && "$1" != -* ]]; then
  output="$1"
  args=("-name" "$1" "${@:2}")
fi

i=0
while [[ $i -lt ${#args[@]} ]]; do
  case "${args[$i]}" in
    -name|-output)
      next=$((i + 1))
      if [[ $next -ge ${#args[@]} ]]; then
        echo "error: ${args[$i]} requires a folder name" >&2
        exit 1
      fi
      output="${args[$next]}"
      ;;
    -name=*|-output=*)
      output="${args[$i]#*=}"
      ;;
  esac
  i=$((i + 1))
done

go run . "${args[@]+"${args[@]}"}"
python3 confidence.py --input-dir "$output"
python3 plot.py --input-dir "$output"
