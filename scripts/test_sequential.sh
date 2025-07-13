#!/usr/bin/env bash
set -e                                   # stop on first error

PAYLOADS=(1 64)
ARRAY_SIZE=(10000 100000 1000000 10000000 50000000 100000000)

mkdir -p results
OUTPUT_FILE="results/sequential.csv"
echo "trial,payload,size,time" > "$OUTPUT_FILE"

for trial in {1..1}; do
  for payload in "${PAYLOADS[@]}"; do
    for size in "${ARRAY_SIZE[@]}"; do
      echo -n "${trial},${payload},${size}," >> "$OUTPUT_FILE"
      ./bin/sequential -p "$payload" -n "$size" \
        | grep 'sort_records' | grep -o '[0-9.]\+' >> "$OUTPUT_FILE"
    done
  done
done
echo "Results saved to $OUTPUT_FILE"
