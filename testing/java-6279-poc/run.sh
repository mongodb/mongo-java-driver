#!/usr/bin/env bash
# Runs the JAVA-6279 proofs of concept.
#
# Usage: ./testing/java-6279-poc/run.sh [primer|driver|executor|all]
#
#   primer    synthetic classes in a throwaway child class loader -- reproduces the finding that a live thread
#             started from a class's static initializer pins that class loader. No driver code.
#   driver    the driver loaded into a child class loader -- shows that BufferPoolPruner keeps that loader, and
#             therefore every driver class and all its static state, strongly reachable after the client is closed.
#   executor  no driver code and no class loaders: checks that a self-terminating, self-resurrecting pruner is
#             implementable on ScheduledThreadPoolExecutor without ever shutting it down. Run under every JDK found
#             on the machine, because it leans on unspecified implementation behaviour and the driver's baseline is
#             Java 8. Set JAVA_HOMES to a colon separated list to override JDK discovery.
#
# No MongoDB server is needed: the driver scenarios never issue an operation.
# Nothing under any module's src/main is modified.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
POC_DIR="$REPO_ROOT/testing/java-6279-poc"
WHAT="${1:-all}"
cd "$REPO_ROOT"

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT
STATUS=0

# ---------------------------------------------------------------------------------------------------------------------
# executor mechanism: standalone, Java 8 clean, no driver jars needed
# ---------------------------------------------------------------------------------------------------------------------
if [ "$WHAT" = "executor" ] || [ "$WHAT" = "all" ]; then
  echo "== compiling the executor mechanism checks at --release 8 =="
  mkdir -p "$OUT/mechanism"
  javac -nowarn --release 8 -d "$OUT/mechanism" "$POC_DIR/src/java6279/ExecutorMechanism.java"

  # Discover JDKs. JAVA_HOMES wins; otherwise ask java_home on macOS; always include whatever java is on the PATH.
  JDKS=""
  if [ -n "${JAVA_HOMES:-}" ]; then
    JDKS="$(echo "$JAVA_HOMES" | tr ':' '\n')"
  elif [ -x /usr/libexec/java_home ]; then
    JDKS="$(/usr/libexec/java_home -V 2>&1 | sed -n 's|.* \(/.*/Contents/Home\)$|\1|p' || true)"
  fi
  JDKS="$(printf '%s\n%s\n' "$JDKS" "$(dirname "$(dirname "$(command -v java)")")" | sed '/^$/d' | sort -u)"

  for JDK in $JDKS; do
    [ -x "$JDK/bin/java" ] || continue
    echo
    echo "############ $("$JDK/bin/java" -version 2>&1 | head -1) ############"
    "$JDK/bin/java" ${POC_JAVA_ARGS:-} -cp "$OUT/mechanism" java6279.ExecutorMechanism || STATUS=1
  done
fi

# ---------------------------------------------------------------------------------------------------------------------
# class loader retention: needs the driver jars
# ---------------------------------------------------------------------------------------------------------------------
if [ "$WHAT" = "primer" ] || [ "$WHAT" = "driver" ] || [ "$WHAT" = "all" ]; then
  need_jar() {
    local pattern="$1"
    local found
    found="$(ls $pattern 2>/dev/null | grep -v -- '-sources' | grep -v -- '-javadoc' | head -1 || true)"
    if [ -z "$found" ]; then
      echo "missing jar: $pattern" >&2
      exit 1
    fi
    echo "$found"
  }

  echo
  if [ -n "${JAVA6279_DRIVER_REPO:-}" ]; then
    # Point the harness at another checkout -- e.g. the backpressure branch in a git worktree -- to see what that
    # tree's threads do. The scenarios are branch agnostic; only the jars under test change.
    echo "== using driver jars from $JAVA6279_DRIVER_REPO =="
    DRIVER_REPO="$JAVA6279_DRIVER_REPO"
  else
    echo "== building driver jars =="
    ./gradlew -q -PskipCryptVerify=true :bson:jar :driver-core:jar :driver-sync:jar
    DRIVER_REPO="$REPO_ROOT"
  fi

  # The classpath handed to the *child* loader. It must be self-contained down to the platform class loader, so it
  # carries the third party jars as well, taken from driver-sync's own runtime classpath so versions match the build.
  DRIVER_CP="$(need_jar "$DRIVER_REPO/bson/build/libs/bson-*.jar")"
  DRIVER_CP="$DRIVER_CP:$(need_jar "$DRIVER_REPO/driver-core/build/libs/mongodb-driver-core-*.jar")"
  DRIVER_CP="$DRIVER_CP:$(need_jar "$DRIVER_REPO/driver-sync/build/libs/mongodb-driver-sync-*.jar")"

  DEPS="$(./gradlew -q -I "$POC_DIR/classpath.gradle" :driver-sync:printRuntimeCp \
          | sed -n 's/^CP://p' | tr ':' '\n' | sort -u | grep -v -- '-SNAPSHOT.jar$' | paste -sd: -)"
  if [ -z "$DEPS" ]; then
    echo "could not resolve third party jars from Gradle" >&2
    exit 1
  fi
  DRIVER_CP="$DRIVER_CP:$DEPS"

  echo "== compiling the class loader proof of concept =="
  find "$POC_DIR/src" -name '*.java' > "$OUT/sources.txt"
  javac -nowarn -d "$OUT/classes" @"$OUT/sources.txt"

  # java6279.Poc goes on the application classpath; java6279.primer.* is also there, but the primer class loader
  # defines it from $OUT/classes itself rather than delegating, which is what gives each scenario a fresh loader.
  echo "== running =="
  SCENARIOS="$WHAT"
  [ "$WHAT" = "all" ] && SCENARIOS="all"
  java -cp "$OUT/classes" \
    -Djava6279.classesDir="$OUT/classes" \
    -Djava6279.driverCp="$DRIVER_CP" \
    ${POC_JAVA_ARGS:-} \
    java6279.Poc "$SCENARIOS" || STATUS=1
fi

exit $STATUS
