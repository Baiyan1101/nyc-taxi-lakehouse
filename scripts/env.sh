#!/usr/bin/env bash

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export JAVA_HOME="$PROJECT_ROOT/.tools/jdk-17.0.19+10"
export SBT_OPTS="-Dsbt.server=false -Dsbt.global.base=$PROJECT_ROOT/.tools/cache/sbt -Dsbt.boot.directory=$PROJECT_ROOT/.tools/cache/sbt/boot -Dsbt.ivy.home=$PROJECT_ROOT/.tools/cache/ivy2 -Divy.home=$PROJECT_ROOT/.tools/cache/ivy2"
export COURSIER_CACHE="$PROJECT_ROOT/.tools/cache/coursier"
export PATH="$PROJECT_ROOT/.tools/sbt/bin:$JAVA_HOME/bin:$PATH"

echo "JAVA_HOME=$JAVA_HOME"
java -version
sbt --script-version
