#!/usr/bin/env bash
set -euo pipefail

rm -rf out
mkdir -p out

find src/main/java -name '*.java' > sources.txt

javac --release 17 -d out @sources.txt
java -cp out com.koshevoi.threadpool.benchmark.TuningStudy
