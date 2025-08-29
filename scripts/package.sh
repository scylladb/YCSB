#!/bin/bash

set -euo pipefail

VERSION="${1:-"0.18.0-SNAPSHOT"}"
JAVA="${2:-21}"

if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  exit 1
fi

rm -rf build

mvn versions:set -DnewVersion="$VERSION"
mvn versions:commit

mvn -ff \
  -am clean package \
  -Djava.source="$JAVA" \
  -Djava.target="$JAVA" \
  -DskipTests

mkdir -p build/conf

tar -xvf "scylla/target/ycsb-scylla-binding-$VERSION.tar.gz" -C build --strip-components=1
tar -xvf "dynamodb/target/ycsb-dynamodb-binding-$VERSION.tar.gz" -C build --strip-components=1

tar -zcvf "scylla-ycsb-$VERSION-java$JAVA.tar.gz" build
