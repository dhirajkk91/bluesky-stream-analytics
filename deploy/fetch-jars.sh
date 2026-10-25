#!/bin/sh
# Downloads the Iceberg jars the Spark job loads into a folder (default .jars),
# checking each against Maven Central's published SHA-1. The version matches
# ICEBERG_VERSION in src/bluesky_stream_analytics/spark.py.
set -eu

VERSION=1.12.0
DEST=${1:-.jars}
BASE=https://repo1.maven.org/maven2/org/apache/iceberg

mkdir -p "$DEST"
for artifact in iceberg-spark-runtime-4.1_2.13 iceberg-aws-bundle; do
    jar="$artifact-$VERSION.jar"
    if [ -f "$DEST/$jar" ]; then
        echo "Already have $jar"
        continue
    fi
    curl -fsSL -o "$DEST/$jar.part" "$BASE/$artifact/$VERSION/$jar"
    expected=$(curl -fsSL "$BASE/$artifact/$VERSION/$jar.sha1" | cut -c1-40)
    if command -v sha1sum >/dev/null 2>&1; then
        actual=$(sha1sum "$DEST/$jar.part" | cut -c1-40)
    else
        actual=$(shasum -a 1 "$DEST/$jar.part" | cut -c1-40)
    fi
    if [ "$expected" != "$actual" ]; then
        rm -f "$DEST/$jar.part"
        echo "Checksum mismatch for $jar" >&2
        exit 1
    fi
    mv "$DEST/$jar.part" "$DEST/$jar"
    echo "Downloaded $jar"
done
